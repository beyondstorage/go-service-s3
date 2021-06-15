package s3

import (
	"context"
	"encoding/base64"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) completeMultipart(ctx context.Context, o *Object, parts []*Part, opt pairStorageCompleteMultipart) (err error) {
	upload := &s3.CompletedMultipartUpload{}
	for _, v := range parts {
		upload.Parts = append(upload.Parts, &s3.CompletedPart{
			ETag: aws.String(v.ETag),
			// For users the `PartNumber` is zero-based. But for S3, the effective `PartNumber` is [1, 10000].
			// Set PartNumber=v.Index+1 here to ensure pass in an effective `PartNumber` for `CompletedPart`.
			PartNumber: aws.Int64(int64(v.Index + 1)),
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

	o.Mode.Del(ModePart)
	o.Mode.Add(ModeRead)
	return
}

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)

	// Handle create multipart object separately.
	if opt.HasMultipartID {
		o = s.newObject(true)
		o.Mode = ModePart
		o.SetMultipartID(opt.MultipartID)
	} else {
		if opt.HasObjectMode && opt.ObjectMode.IsDir() {
			rp += "/"
			o = s.newObject(true)
			o.Mode = ModeDir
		} else {
			o = s.newObject(false)
			o.Mode = ModeRead
		}
	}
	o.ID = rp
	o.Path = path
	return o
}

func (s *Storage) createDir(ctx context.Context, path string, opt pairStorageCreateDir) (o *Object, err error) {
	rp := s.getAbsPath(path)

	// Add `/` at the end of `path` to simulate a directory.
	//ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html
	rp += "/"

	var size int64 = 0
	input := &s3.PutObjectInput{
		Bucket:        aws.String(s.name),
		Key:           aws.String(rp),
		ContentLength: &size,
		Body:          aws.ReadSeekCloser(nil),
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
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
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
		input.ServerSideEncryption = &opt.ServerSideEncryption
	}

	_, err = s.service.PutObjectWithContext(ctx, input)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.Mode = ModeDir
	o.ID = rp
	o.Path = path
	return o, nil
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
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
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
	// set multipart restriction
	o.SetMultipartNumberMaximum(multipartNumberMaximum)
	o.SetMultipartSizeMaximum(multipartSizeMaximum)
	o.SetMultipartSizeMinimum(multipartSizeMinimum)

	var sm ObjectMetadata
	if v := aws.StringValue(output.ServerSideEncryption); v != "" {
		sm.ServerSideEncryption = v
	}
	if v := aws.StringValue(output.SSEKMSKeyId); v != "" {
		sm.ServerSideEncryptionAwsKmsKeyID = v
	}
	if v := aws.StringValue(output.SSEKMSEncryptionContext); v != "" {
		sm.ServerSideEncryptionContext = v
	}
	if v := aws.StringValue(output.SSECustomerAlgorithm); v != "" {
		sm.ServerSideEncryptionCustomerAlgorithm = v
	}
	if v := aws.StringValue(output.SSECustomerKeyMD5); v != "" {
		sm.ServerSideEncryptionCustomerKeyMd5 = v
	}
	if output.BucketKeyEnabled != nil {
		sm.ServerSideEncryptionBucketKeyEnabled = aws.BoolValue(output.BucketKeyEnabled)
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

		// S3 AbortMultipartUpload is idempotent, so we don't need to check NoSuchUpload error.
		//
		// References
		// - [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
		// - https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
		_, err = s.service.AbortMultipartUpload(abortInput)
		if err != nil {
			return
		}
	}

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		rp += "/"
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}

	// S3 DeleteObject is idempotent, so we don't need to check NoSuchKey error.
	//
	// References
	// - [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
	// - https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
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
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) listMultipart(ctx context.Context, o *Object, opt pairStorageListMultipart) (pi *PartIterator, err error) {
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

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.Name = s.name
	meta.WorkDir = s.workDir
	return meta
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
			// The returned `PartNumber` is [1, 10000].
			// Set Index=*v.PartNumber-1 here to make the `PartNumber` zero-based for user.
			Index: int(*v.PartNumber) - 1,
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
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
		}
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

	if opt.HasMultipartID {
		listInput := &s3.ListPartsInput{
			Bucket:   aws.String(s.name),
			Key:      aws.String(rp),
			UploadId: aws.String(opt.MultipartID),
		}
		if opt.HasExceptedBucketOwner {
			listInput.ExpectedBucketOwner = &opt.ExceptedBucketOwner
		}

		_, err = s.service.ListPartsWithContext(ctx, listInput)
		if err != nil {
			return nil, err
		}

		o = s.newObject(true)
		o.ID = rp
		o.Path = path
		o.Mode.Add(ModePart)
		o.SetMultipartID(opt.MultipartID)
		return o, nil
	}

	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		rp += "/"
	}

	input := &s3.HeadObjectInput{
		Bucket: aws.String(s.name),
		Key:    aws.String(rp),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
		}
	}

	output, err := s.service.HeadObject(input)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	if opt.HasObjectMode && opt.ObjectMode.IsDir() {
		o.Mode |= ModeDir
	} else {
		o.Mode |= ModeRead
	}

	o.SetContentLength(aws.Int64Value(output.ContentLength))
	o.SetLastModified(aws.TimeValue(output.LastModified))

	if output.ContentType != nil {
		o.SetContentType(*output.ContentType)
	}
	if output.ETag != nil {
		o.SetEtag(*output.ETag)
	}

	var sm ObjectMetadata
	if v := aws.StringValue(output.StorageClass); v != "" {
		sm.StorageClass = v
	}
	if v := aws.StringValue(output.ServerSideEncryption); v != "" {
		sm.ServerSideEncryption = v
	}
	if v := aws.StringValue(output.SSEKMSKeyId); v != "" {
		sm.ServerSideEncryptionAwsKmsKeyID = v
	}
	if v := aws.StringValue(output.SSECustomerAlgorithm); v != "" {
		sm.ServerSideEncryptionCustomerAlgorithm = v
	}
	if v := aws.StringValue(output.SSECustomerKeyMD5); v != "" {
		sm.ServerSideEncryptionCustomerKeyMd5 = v
	}
	if output.BucketKeyEnabled != nil {
		sm.ServerSideEncryptionBucketKeyEnabled = aws.BoolValue(output.BucketKeyEnabled)
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
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
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
		input.ServerSideEncryption = &opt.ServerSideEncryption
	}

	_, err = s.service.PutObjectWithContext(ctx, input)
	if err != nil {
		return
	}
	return size, nil
}

func (s *Storage) writeMultipart(ctx context.Context, o *Object, r io.Reader, size int64, index int, opt pairStorageWriteMultipart) (n int64, part *Part, err error) {
	input := &s3.UploadPartInput{
		Bucket: &s.name,
		// For S3, the `PartNumber` is [1, 10000]. But for users, the `PartNumber` is zero-based.
		// Set PartNumber=index+1 here to ensure pass in an effective `PartNumber` for `UploadPart`.
		// ref: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
		PartNumber:    aws.Int64(int64(index + 1)),
		Key:           aws.String(o.ID),
		UploadId:      aws.String(o.MustGetMultipartID()),
		ContentLength: &size,
		Body:          iowrap.SizedReadSeekCloser(r, size),
	}
	if opt.HasExceptedBucketOwner {
		input.ExpectedBucketOwner = &opt.ExceptedBucketOwner
	}
	if opt.HasServerSideEncryptionCustomerAlgorithm {
		input.SSECustomerAlgorithm, input.SSECustomerKey, input.SSECustomerKeyMD5, err = calculateEncryptionHeaders(opt.ServerSideEncryptionCustomerAlgorithm, opt.ServerSideEncryptionCustomerKey)
		if err != nil {
			return
		}
	}

	output, err := s.service.UploadPartWithContext(ctx, input)
	if err != nil {
		return
	}

	part = &Part{
		Index: index,
		Size:  size,
		ETag:  aws.StringValue(output.ETag),
	}
	return size, part, nil
}
