- Author: abyss-w <mad.hatter@foxmail.com>
- Start Date: 2021-08-11
- RFC PR: [beyondstorage/go-service-s3#178](https://github.com/beyondstorage/go-service-s3/pull/178)
- Tracking Issue: [beyondstorage/go-service-s3#144](https://github.com/beyondstorage/go-service-s3/issues/144)

# RFC-178: Add Virtual Link Support

## Background

Like the one presented in [GSP-86 Add Create Link Operation](https://github.com/beyondstorage/go-storage/blob/master/docs/rfcs/86-add-create-link-operation.md), s3 has no native support for symlink. We can use [x-amz-website-redirect-location](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-page-redirect.html) redirect pages but only works for a website. 

## Proposal

I propose to implement virtual_link feature to support symlink of the website in s3.

```go
input := &s3.PutObjectInput{
    WebsiteRedirectLocation: &rt,
}
```

- `PutObjectInput` in s3 is used to store the fields we need when calling `PutObjectWithContext` API to upload an object.
- `rt` is the symlink target, it is an absolute path.
- `WebsiteRedirectLocation` is used to configure redirects for the objects.
  - This is only works if the Amazon S3 bucket is configured to host a static website.

## Rationale

### System-defined object metadata & WebsiteRedirectLocation

As the [official S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html) and [Configuring a redirect](https://docs.aws.amazon.com/AmazonS3/latest/userguide/how-to-page-redirect.html) says, we can use the system-defined object metadata `x-amz-website-redirect-location` to redirect requests for the associated object to another object in the same bucket or an external URL. This allows us to simulate symlink on the website.

We use the Amazon S3 API, so We can configure `x-amz-website-redirect-location` by setting the `WebsiteRedirectLocation`. The website then interprets the object as a 301 redirect.

### Drawbacks

As s3 itself does not support symlink, we can only simulate it. Based on the existing functionality of s3, we can only implement symlink on static website. And the object created is not really a symlink object. When we call stat and list, we can only tell if it is a symlink by using system-defined metadata `WebsiteRedirectLocation`.

```go
if WebsiteRedirectLocation != nil {
    // The path is a symlink object.
}
```

## Compatibility

N/A

## Implementation

- Implement `virtual_link` in go-service-s3
- Support `stat`/`list`
- Setup linker tests

