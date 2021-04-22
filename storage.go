package s3

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aos-dev/go-storage/v3/pkg/iowrap"
	. "github.com/aos-dev/go-storage/v3/types"
)

func (s *Storage) completeMultipart(ctx context.Context, o *Object, parts []*Part, opt pairStorageCompleteMultipart) (err error) {
	if o.Mode&ModePart == 0 {
		return fmt.Errorf("object is not a part object")
	}

	upload := &s3.CompletedMultipartUpload{}
	for _, v := range parts {
		upload.Parts = append(upload.Parts, &s3.CompletedPart{
			ETag:       aws.String(v.ETag),
			PartNumber: aws.Int64(int64(v.Index)),
		})
	}

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(s.name),
		Key:             aws.String(o.ID),
		MultipartUpload: upload,
		UploadId:        aws.String(o.MustGetMultipartID()),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}

	_, err = s.service.CompleteMultipartUploadWithContext(ctx, input)
	if err != nil {
		return
	}
	return
}

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	o = s.newObject(false)
	o.Mode = ModeRead
	o.ID = s.getAbsPath(path)
	o.Path = path
	return o
}

func (s *Storage) createMultipart(ctx context.Context, path string, opt pairStorageCreateMultipart) (o *Object, err error) {
	rp := s.getAbsPath(path)

	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasServerSideEncryptionBucketKeyEnabled {
		input.BucketKeyEnabled = &opt.ServerSideEncryptionBucketKeyEnabled
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm = &opt.ServerSideEncryptionCustomerAlgorithm
	}
	if opt.HasServerSideEncryptionCustomerKey {
		encodeCustomerKey := base64.StdEncoding.EncodeToString(opt.ServerSideEncryptionCustomerKey)
		input.SSECustomerKey = &encodeCustomerKey

		data := md5.Sum(opt.ServerSideEncryptionCustomerKey)
		keyMD5 := hex.EncodeToString(data[:])
		encodedCustomerKeyMD5 := base64.StdEncoding.EncodeToString([]byte(keyMD5))
		input.SSECustomerKeyMD5 = &encodedCustomerKeyMD5
	}
	if opt.HasServerSideEncryptionAwsKmsKeyID {
		input.SSEKMSKeyId = &opt.ServerSideEncryptionAwsKmsKeyID
	}
	if opt.HasServerSideEncryptionContext {
		encodedKMSEncryptionContext := base64.StdEncoding.EncodeToString([]byte(opt.ServerSideEncryptionContext))
		input.SSEKMSEncryptionContext = &encodedKMSEncryptionContext
	}
	if opt.HasServerSideEncryption {
		input.ServerSideEncryption = &opt.ServerSideEncryption
	}

	output, err := s.service.CreateMultipartUpload(input)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModePart
	o.SetMultipartID(aws.StringValue(output.UploadId))

	sm := make(map[string]string)
	if v := aws.StringValue(output.ServerSideEncryption); v != "" {
		sm[MetadataServerSideEncryption] = v
	}
	if v := aws.StringValue(output.SSEKMSKeyId); v != "" {
		sm[MetadataServerSideEncryptionAwsKmsKeyID] = v
	}
	if v := aws.StringValue(output.SSEKMSEncryptionContext); v != "" {
		sm[MetadataServerSideEncryptionContext] = v
	}
	if v := aws.StringValue(output.SSECustomerAlgorithm); v != "" {
		sm[MetadataServerSideEncryptionCustomerAlgorithm] = v
	}
	if v := aws.StringValue(output.SSECustomerKeyMD5); v != "" {
		sm[MetadataServerSideEncryptionCustomerKeyMd5] = v
	}
	o.SetServiceMetadata(sm)

	return o, nil
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	if opt.HasMultipartID {
		abortInput := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(s.name),
			Key:      aws.String(rp),
			UploadId: aws.String(opt.MultipartID),
		}
		if opt.HasExceptedBucketOwner {
			abortInput.ExpectedBucketOwner = &opt.ExceptedBucketOwner
		}
		_, err = s.service.AbortMultipartUpload(abortInput)
		if err != nil {
			return
		}
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}

	_, err = s.service.DeleteObject(input)
	if err != nil {
		return err
	}
	return nil
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		maxKeys: 200,
		prefix:  s.getAbsPath(path),
	}

	if opt.HasExceptedBucketOwner {
		input.expectedBucketOwner = opt.ExceptedBucketOwner
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsPart():
		nextFn = s.nextPartObjectPageByPrefix
	case opt.ListMode.IsDir():
		input.delimiter = "/"
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, fmt.Errorf("invalid list mode")
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) listMultipart(ctx context.Context, o *Object, opt pairStorageListMultipart) (pi *PartIterator, err error) {
	if o.Mode&ModePart == 0 {
		return nil, fmt.Errorf("object is not a part object")
	}

	input := &partPageStatus{
		maxParts: 200,
		key:      o.ID,
		uploadId: o.MustGetMultipartID(),
	}
	if opt.HasExceptedBucketOwner {
		input.expectedBucketOwner = opt.ExceptedBucketOwner
	}

	return NewPartIterator(ctx, s.nextPartPage, input), nil
}

func (s *Storage) metadata(ctx context.Context, opt pairStorageMetadata) (meta *StorageMeta, err error) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta, nil
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	listInput := &s3.ListObjectsV2Input{
		Bucket:            &s.name,
		Delimiter:         &input.delimiter,
		MaxKeys:           &input.maxKeys,
		ContinuationToken: input.getServiceContinuationToken(),
		Prefix:            &input.prefix,
	}
	if input.expectedBucketOwner != "" {
		listInput.ExpectedBucketOwner = &input.expectedBucketOwner
	}

	output, err := s.service.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return err
	}

	for _, v := range output.CommonPrefixes {
		o := s.newObject(true)
		o.ID = *v.Prefix
		o.Path = s.getRelPath(*v.Prefix)
		o.Mode |= ModeDir

		page.Data = append(page.Data, o)
	}

	for _, v := range output.Contents {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if !aws.BoolValue(output.IsTruncated) {
		return IterateDone
	}

	input.continuationToken = *output.NextContinuationToken
	return nil
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	listInput := &s3.ListObjectsV2Input{
		Bucket:            &s.name,
		MaxKeys:           &input.maxKeys,
		ContinuationToken: input.getServiceContinuationToken(),
		Prefix:            &input.prefix,
	}
	if input.expectedBucketOwner != "" {
		listInput.ExpectedBucketOwner = &input.expectedBucketOwner
	}

	output, err := s.service.ListObjectsV2WithContext(ctx, listInput)
	if err != nil {
		return err
	}

	for _, v := range output.Contents {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if !aws.BoolValue(output.IsTruncated) {
		return IterateDone
	}

	input.continuationToken = aws.StringValue(output.NextContinuationToken)
	return nil
}

func (s *Storage) nextPartObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	listInput := &s3.ListMultipartUploadsInput{
		Bucket:         &s.name,
		KeyMarker:      &input.keyMarker,
		MaxUploads:     &input.maxKeys,
		Prefix:         &input.prefix,
		UploadIdMarker: &input.uploadIdMarker,
	}
	if input.expectedBucketOwner != "" {
		listInput.ExpectedBucketOwner = &input.expectedBucketOwner
	}

	output, err := s.service.ListMultipartUploadsWithContext(ctx, listInput)
	if err != nil {
		return err
	}

	for _, v := range output.Uploads {
		o := s.newObject(true)
		o.ID = *v.Key
		o.Path = s.getRelPath(*v.Key)
		o.Mode |= ModePart
		o.SetMultipartID(*v.UploadId)

		page.Data = append(page.Data, o)
	}

	if !aws.BoolValue(output.IsTruncated) {
		return IterateDone
	}

	input.keyMarker = aws.StringValue(output.KeyMarker)
	input.uploadIdMarker = aws.StringValue(output.UploadIdMarker)
	return nil
}

func (s *Storage) nextPartPage(ctx context.Context, page *PartPage) error {
	input := page.Status.(*partPageStatus)

	listInput := &s3.ListPartsInput{
		Bucket:           &s.name,
		Key:              &input.key,
		MaxParts:         &input.maxParts,
		PartNumberMarker: &input.partNumberMarker,
		UploadId:         &input.uploadId,
	}
	if input.expectedBucketOwner != "" {
		listInput.ExpectedBucketOwner = &input.expectedBucketOwner
	}

	output, err := s.service.ListPartsWithContext(ctx, listInput)
	if err != nil {
		return err
	}

	for _, v := range output.Parts {
		p := &Part{
			Index: int(*v.PartNumber),
			Size:  *v.Size,
			ETag:  aws.StringValue(v.ETag),
		}

		page.Data = append(page.Data, p)
	}

	if !aws.BoolValue(output.IsTruncated) {
		return IterateDone
	}

	input.partNumberMarker = aws.Int64Value(output.NextPartNumberMarker)
	return nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)

	input := &s3.GetObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm = &opt.ServerSideEncryptionCustomerAlgorithm
	}
	if opt.HasServerSideEncryptionCustomerKey {
		encodeCustomerKey := base64.StdEncoding.EncodeToString(opt.ServerSideEncryptionCustomerKey)
		input.SSECustomerKey = &encodeCustomerKey

		data := md5.Sum(opt.ServerSideEncryptionCustomerKey)
		keyMD5 := hex.EncodeToString(data[:])
		encodeCustomerKeyMD5 := base64.StdEncoding.EncodeToString([]byte(keyMD5))
		input.SSECustomerKeyMD5 = &encodeCustomerKeyMD5
	}

	output, err := s.service.GetObjectWithContext(ctx, input)
	if err != nil {
		return
	}
	defer output.Body.Close()

	rc := output.Body
	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm = &opt.ServerSideEncryptionCustomerAlgorithm
	}
	if opt.HasServerSideEncryptionCustomerKey {
		encodeCustomerKey := base64.StdEncoding.EncodeToString(opt.ServerSideEncryptionCustomerKey)
		input.SSECustomerKey = &encodeCustomerKey

		data := md5.Sum(opt.ServerSideEncryptionCustomerKey)
		keyMD5 := hex.EncodeToString(data[:])
		encodeCustomerKeyMD5 := base64.StdEncoding.EncodeToString([]byte(keyMD5))
		input.SSECustomerKeyMD5 = &encodeCustomerKeyMD5
	}

	output, err := s.service.HeadObject(input)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModeRead

	o.SetContentLength(aws.Int64Value(output.ContentLength))
	o.SetLastModified(aws.TimeValue(output.LastModified))

	if output.ContentType != nil {
		o.SetContentType(*output.ContentType)
	}
	if output.ETag != nil {
		o.SetEtag(*output.ETag)
	}
	sm := make(map[string]string)
	if v := aws.StringValue(output.StorageClass); v != "" {
		sm[MetadataStorageClass] = v
	}
	if v := aws.StringValue(output.ServerSideEncryption); v != "" {
		sm[MetadataServerSideEncryption] = v
	}
	if v := aws.StringValue(output.SSEKMSKeyId); v != "" {
		sm[MetadataServerSideEncryptionAwsKmsKeyID] = v
	}
	if v := aws.StringValue(output.SSECustomerAlgorithm); v != "" {
		sm[MetadataServerSideEncryptionCustomerAlgorithm] = v
	}
	if v := aws.StringValue(output.SSECustomerKeyMD5); v != "" {
		sm[MetadataServerSideEncryptionCustomerKeyMd5] = v
	}
	o.SetServiceMetadata(sm)

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	rp := s.getAbsPath(path)

	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.name),
		Key:           aws.String(rp),
		ContentLength: &size,
		Body:          aws.ReadSeekCloser(r),
	}
	if opt.HasContentMd5 {
		input.ContentMD5 = &opt.ContentMd5
	}
	if opt.HasStorageClass {
		input.StorageClass = &opt.StorageClass
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionBucketKeyEnabled {
		input.BucketKeyEnabled = &opt.ServerSideEncryptionBucketKeyEnabled
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm = &opt.ServerSideEncryptionCustomerAlgorithm
	}
	if opt.HasServerSideEncryptionCustomerKey {
		encodeCustomerKey := base64.StdEncoding.EncodeToString(opt.ServerSideEncryptionCustomerKey)
		input.SSECustomerKey = &encodeCustomerKey

		data := md5.Sum(opt.ServerSideEncryptionCustomerKey)
		keyMD5 := hex.EncodeToString(data[:])
		encodeCustomerKeyMD5 := base64.StdEncoding.EncodeToString([]byte(keyMD5))
		input.SSECustomerKeyMD5 = &encodeCustomerKeyMD5
	}
	if opt.HasServerSideEncryptionAwsKmsKeyID {
		input.SSEKMSKeyId = &opt.ServerSideEncryptionAwsKmsKeyID
	}
	if opt.HasServerSideEncryptionContext {
		encodedKMSEncryptionContext := base64.StdEncoding.EncodeToString([]byte(opt.ServerSideEncryptionContext))
		input.SSEKMSEncryptionContext = &encodedKMSEncryptionContext
	}
	if opt.HasServerSideEncryption {
		input.ServerSideEncryption = &opt.ServerSideEncryption
	}

	_, err = s.service.PutObjectWithContext(ctx, input)
	if err != nil {
		return
	}
	return size, nil
}

func (s *Storage) writeMultipart(ctx context.Context, o *Object, r io.Reader, size int64, index int, opt pairStorageWriteMultipart) (n int64, err error) {
	if o.Mode&ModePart == 0 {
		return 0, fmt.Errorf("object is not a part object")
	}

	input := &s3.UploadPartInput{
		Bucket:        &s.name,
		PartNumber:    aws.Int64(int64(index)),
		Key:           aws.String(o.ID),
		UploadId:      aws.String(o.MustGetMultipartID()),
		ContentLength: &size,
		Body:          iowrap.SizedReadSeekCloser(r, size),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm = &opt.ServerSideEncryptionCustomerAlgorithm
	}
	if opt.HasServerSideEncryptionCustomerKey {
		encodeCustomerKey := base64.StdEncoding.EncodeToString(opt.ServerSideEncryptionCustomerKey)
		input.SSECustomerKey = &encodeCustomerKey

		data := md5.Sum(opt.ServerSideEncryptionCustomerKey)
		keyMD5 := hex.EncodeToString(data[:])
		encodeCustomerKeyMD5 := base64.StdEncoding.EncodeToString([]byte(keyMD5))
		input.SSECustomerKeyMD5 = &encodeCustomerKeyMD5
	}

	_, err = s.service.UploadPartWithContext(ctx, input)
	if err != nil {
		return
	}
	return size, nil
}
