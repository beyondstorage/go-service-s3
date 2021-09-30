package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/aws/smithy-go/middleware"
	ps "github.com/beyondstorage/go-storage/v4/pairs"
	"github.com/beyondstorage/go-storage/v4/pkg/credential"
	"github.com/beyondstorage/go-storage/v4/pkg/httpclient"
	"github.com/beyondstorage/go-storage/v4/services"
	typ "github.com/beyondstorage/go-storage/v4/types"
)

// Service is the s3 service config.
type Service struct {
	cfg          *aws.Config
	service      *s3.Client
	defaultPairs DefaultServicePairs
	features     ServiceFeatures

	typ.UnimplementedServicer
}

// String implements Servicer.String
func (s *Service) String() string {
	return fmt.Sprintf("Servicer s3")
}

// Storage is the s3 object storage service.
type Storage struct {
	service *s3.Client

	name    string
	workDir string

	defaultPairs DefaultStoragePairs
	features     StorageFeatures

	typ.UnimplementedStorager
	typ.UnimplementedDirer
	typ.UnimplementedMultiparter
	typ.UnimplementedLinker
	typ.UnimplementedStorageHTTPSigner
}

// String implements Storager.String
func (s *Storage) String() string {
	return fmt.Sprintf(
		"Storager s3 {Name: %s, WorkDir: %s}",
		s.name, s.workDir,
	)
}

// New will create both Servicer and Storager.
func New(pairs ...typ.Pair) (typ.Servicer, typ.Storager, error) {
	return newServicerAndStorager(pairs...)
}

// NewServicer will create Servicer only.
func NewServicer(pairs ...typ.Pair) (typ.Servicer, error) {
	return newServicer(pairs...)
}

// NewStorager will create Storager only.
func NewStorager(pairs ...typ.Pair) (typ.Storager, error) {
	_, store, err := newServicerAndStorager(pairs...)
	return store, err
}

func newServicer(pairs ...typ.Pair) (srv *Service, err error) {
	defer func() {
		if err != nil {
			err = services.InitError{Op: "new_servicer", Type: Type, Err: formatError(err), Pairs: pairs}
		}
	}()

	opt, err := parsePairServiceNew(pairs)
	if err != nil {
		return nil, err
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(opt.Location))
	if err != nil {
		return nil, err
	}

	// Set s3 config's http client
	cfg.HTTPClient = httpclient.New(opt.HTTPClientOptions)

	cp, err := credential.Parse(opt.Credential)
	if err != nil {
		return nil, err
	}
	switch cp.Protocol() {
	case credential.ProtocolHmac:
		ak, sk := cp.Hmac()
		cfg.Credentials = aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(ak, sk, ""))
	default:
		return nil, services.PairUnsupportedError{Pair: ps.WithCredential(opt.Credential)}
	}

	srv = &Service{
		cfg:     &cfg,
		service: newS3Service(&cfg, opt),
	}

	if opt.HasDefaultServicePairs {
		srv.defaultPairs = opt.DefaultServicePairs
	}
	if opt.HasServiceFeatures {
		srv.features = opt.ServiceFeatures
	}
	return
}

// New will create a new s3 service.
func newServicerAndStorager(pairs ...typ.Pair) (srv *Service, store *Storage, err error) {
	srv, err = newServicer(pairs...)
	if err != nil {
		return
	}

	store, err = srv.newStorage(pairs...)
	if err != nil {
		err = services.InitError{Op: "new_storager", Type: Type, Err: formatError(err), Pairs: pairs}
		return
	}
	return
}

// All available storage classes are listed here.
const (
	StorageClassStandard           = types.ObjectStorageClassStandard
	StorageClassReducedRedundancy  = types.ObjectStorageClassReducedRedundancy
	StorageClassGlacier            = types.ObjectStorageClassGlacier
	StorageClassStandardIa         = types.ObjectStorageClassStandardIa
	StorageClassOnezoneIa          = types.ObjectStorageClassOnezoneIa
	StorageClassIntelligentTiering = types.ObjectStorageClassIntelligentTiering
	StorageClassDeepArchive        = types.ObjectStorageClassDeepArchive
)

func formatError(err error) error {
	if _, ok := err.(services.InternalError); ok {
		return err
	}

	fmt.Printf("%T", err)
	e := &awshttp.ResponseError{}
	if ok := errors.As(err, &e); !ok {
		return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
	}

	switch err.Error() {
	// AWS SDK will use status code to generate awserr.Error, so "NotFound" should also be supported.
	case "NoSuchKey", "NotFound":
		return fmt.Errorf("%w: %v", services.ErrObjectNotExist, err)
	case "AccessDenied":
		return fmt.Errorf("%w: %v", services.ErrPermissionDenied, err)
	default:
		return fmt.Errorf("%w: %v", services.ErrUnexpected, err)
	}
}

func newS3Service(cfgs *aws.Config, opt pairServiceNew) (srv *s3.Client) {
	srv = s3.NewFromConfig(*cfgs, func(options *s3.Options) {
		options.Region = opt.Location
		options.APIOptions = append(options.APIOptions,
			func(stack *middleware.Stack) error {
				v4.RemoveComputePayloadSHA256Middleware(stack)
				v4.AddUnsignedPayloadMiddleware(stack)
				v4.RemoveContentSHA256HeaderMiddleware(stack)
				return v4.AddContentSHA256HeaderMiddleware(stack)
			})
	})

	return
}

// newStorage will create a new client.
func (s *Service) newStorage(pairs ...typ.Pair) (st *Storage, err error) {
	optStorage, err := parsePairStorageNew(pairs)
	if err != nil {
		return nil, err
	}

	optService, err := parsePairServiceNew(pairs)
	if err != nil {
		return nil, err
	}
	st = &Storage{
		service: newS3Service(s.cfg, optService),
		name:    optStorage.Name,
		workDir: "/",
	}

	if optStorage.HasDefaultStoragePairs {
		st.defaultPairs = optStorage.DefaultStoragePairs
	}
	if optStorage.HasStorageFeatures {
		st.features = optStorage.StorageFeatures
	}
	if optStorage.HasWorkDir {
		st.workDir = optStorage.WorkDir
	}
	return st, nil
}

func (s *Service) formatError(op string, err error, name string) error {
	if err == nil {
		return nil
	}

	return services.ServiceError{
		Op:       op,
		Err:      formatError(err),
		Servicer: s,
		Name:     name,
	}
}

// getAbsPath will calculate object storage's abs path
func (s *Storage) getAbsPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return prefix + path
}

// getRelPath will get object storage's rel path.
func (s *Storage) getRelPath(path string) string {
	prefix := strings.TrimPrefix(s.workDir, "/")
	return strings.TrimPrefix(path, prefix)
}

func (s *Storage) formatError(op string, err error, path ...string) error {
	if err == nil {
		return nil
	}

	return services.StorageError{
		Op:       op,
		Err:      formatError(err),
		Storager: s,
		Path:     path,
	}
}

func (s *Storage) formatFileObject(v *types.Object) (o *typ.Object, err error) {
	o = s.newObject(false)
	o.ID = *v.Key
	o.Path = s.getRelPath(*v.Key)
	// If you have enabled virtual link, you will not get the accurate object type.
	// If you want to get the exact object mode, please use `stat`
	o.Mode |= typ.ModeRead

	o.SetContentLength(*aws.Int64(v.Size))
	o.SetLastModified(*aws.Time(*v.LastModified))

	if v.ETag != nil {
		o.SetEtag(*v.ETag)
	}

	var sm ObjectSystemMetadata
	if value := string(v.StorageClass); value != "" {
		sm.StorageClass = value
	}
	o.SetSystemMetadata(sm)

	return
}

func (s *Storage) newObject(done bool) *typ.Object {
	return typ.NewObject(s, done)
}

// All available server side algorithm are listed here.
const (
	ServerSideEncryptionAes256 = types.ServerSideEncryptionAes256
	ServerSideEncryptionAwsKms = types.ServerSideEncryptionAwsKms
)

func calculateEncryptionHeaders(algo string, key []byte) (algorithm, keyBase64, keyMD5Base64 *string, err error) {
	if len(key) != 32 {
		err = ErrServerSideEncryptionCustomerKeyInvalid
		return
	}
	kB64 := base64.StdEncoding.EncodeToString(key)
	kMD5 := md5.Sum(key)
	kMD5B64 := base64.StdEncoding.EncodeToString(kMD5[:])
	return &algo, &kB64, &kMD5B64, nil
}

// multipartXXX are multipart upload restriction in S3, see more details at:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
const (
	// multipartNumberMaximum is the max part count supported.
	multipartNumberMaximum = 10000
	// multipartSizeMaximum is the maximum size for each part, 5GB.
	multipartSizeMaximum = 5 * 1024 * 1024 * 1024
	// multipartSizeMinimum is the minimum size for each part, 5MB.
	multipartSizeMinimum = 5 * 1024 * 1024
)

const (
	// writeSizeMaximum is the maximum size for each object with a single PUT operation, 5GB.
	// ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html
	writeSizeMaximum = 5 * 1024 * 1024 * 1024
)

func (s *Storage) formatGetObjectInput(path string, opt pairStorageRead) (input *s3.GetObjectInput, err error) {
	rp := s.getAbsPath(path)

	input = &s3.GetObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}

	if opt.HasOffset && opt.HasSize {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", opt.Offset, opt.Offset+opt.Size-1))
	} else if opt.HasOffset && !opt.HasSize {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", opt.Offset))
	} else if !opt.HasOffset && opt.HasSize {
		input.Range = aws.String(fmt.Sprintf("bytes=0-%d", opt.Size-1))
	}

	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return nil, err
		}
	}

	return
}

func (s *Storage) formatPutObjectInput(path string, size int64, opt pairStorageWrite) (input *s3.PutObjectInput, err error) {
	rp := s.getAbsPath(path)

	input = &s3.PutObjectInput{
		Bucket:        aws.String(s.name),
		Key:           aws.String(rp),
		ContentLength: *aws.Int64(size),
	}

	if opt.HasContentMd5 {
		input.ContentMD5 = &opt.ContentMd5
	}
	if opt.HasStorageClass {
		input.StorageClass = types.StorageClass(opt.StorageClass)
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionBucketKeyEnabled {
		input.BucketKeyEnabled = opt.ServerSideEncryptionBucketKeyEnabled
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return nil, err
		}
	}
	if opt.HasServerSideEncryptionAwsKmsKeyID {
		input.SSEKMSKeyId = &opt.ServerSideEncryptionAwsKmsKeyID
	}
	if opt.HasServerSideEncryptionContext {
		encodedKMSEncryptionContext := base64.StdEncoding.EncodeToString([]byte(opt.ServerSideEncryptionContext))
		input.SSEKMSEncryptionContext = &encodedKMSEncryptionContext
	}
	if opt.HasServerSideEncryption {
		input.ServerSideEncryption = types.ServerSideEncryption(opt.ServerSideEncryption)
	}

	return
}
