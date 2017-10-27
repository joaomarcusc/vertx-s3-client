/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.s3.model;

import javax.xml.bind.annotation.XmlEnum;
import javax.xml.bind.annotation.XmlEnumValue;
import javax.xml.bind.annotation.XmlType;

/**
 * @author Emir Dizdarevic
 * @since 3.3.0
 */
@XmlType
@XmlEnum(String.class)
public enum ErrorCode {
    @XmlEnumValue("AccessDenied")ACCESS_DENIED("AccessDenied"),
    @XmlEnumValue("AccountProblem")ACCOUNT_PROBLEM("AccountProblem"),
    @XmlEnumValue("AmbiguousGrantByEmailAddress")AMBIGUOUS_GRANT_BY_EMAIL_ADDRESS("AmbiguousGrantByEmailAddress"),
    @XmlEnumValue("BadDigest")BAD_DIGEST("BadDigest"),
    @XmlEnumValue("BucketAlreadyExists")BUCKET_ALREADY_EXISTS("BucketAlreadyExists"),
    @XmlEnumValue("BucketAlreadyOwnedByYou")BUCKET_ALREADY_OWNED_BY_YOU("BucketAlreadyOwnedByYou"),
    @XmlEnumValue("BucketNotEmpty")BUCKET_NOT_EMPTY("BucketNotEmpty"),
    @XmlEnumValue("CredentialsNotSupported")CREDENTIALS_NOT_SUPPORTED("CredentialsNotSupported"),
    @XmlEnumValue("CrossLocationLoggingProhibited")CROSS_LOCATION_LOGGING_PROHIBITED("CrossLocationLoggingProhibited"),
    @XmlEnumValue("EntityTooSmall")ENTITY_TOO_SMALL("EntityTooSmall"),
    @XmlEnumValue("EntityTooLarge")ENTITY_TOO_LARGE("EntityTooLarge"),
    @XmlEnumValue("ExpiredToken")EXPIRED_TOKEN("ExpiredToken"),
    @XmlEnumValue("IllegalVersioningConfigurationException")ILLEGAL_VERSIONING_CONFIGURATION_EXCEPTION("IllegalVersioningConfigurationException"),
    @XmlEnumValue("IncompleteBody")INCOMPLETE_BODY("IncompleteBody"),
    @XmlEnumValue("IncorrectNumberOfFilesInPostRequest")INCORRECT_NUMBER_OF_FILES_IN_POST_REQUEST("IncorrectNumberOfFilesInPostRequest"),
    @XmlEnumValue("InlineDataTooLarge")INLINE_DATA_TOO_LARGE("InlineDataTooLarge"),
    @XmlEnumValue("InternalError")INTERNAL_ERROR("InternalError"),
    @XmlEnumValue("InvalidAccessKeyId")INVALID_ACCESS_KEY_ID("InvalidAccessKeyId"),
    @XmlEnumValue("InvalidAddressingHeader")INVALID_ADDRESSING_HEADER("InvalidAddressingHeader"),
    @XmlEnumValue("InvalidArgument")INVALID_ARGUMENT("InvalidArgument"),
    @XmlEnumValue("InvalidBucketName")INVALID_BUCKET_NAME("InvalidBucketName"),
    @XmlEnumValue("InvalidBucketState")INVALID_BUCKET_STATE("InvalidBucketState"),
    @XmlEnumValue("InvalidDigest")INVALID_DIGEST("InvalidDigest"),
    @XmlEnumValue("InvalidEncryptionAlgorithmError")INVALID_ENCRYPTION_ALGORITHM_ERROR("InvalidEncryptionAlgorithmError"),
    @XmlEnumValue("InvalidLocationConstraint")INVALID_LOCATION_CONSTRAINT("InvalidLocationConstraint"),
    @XmlEnumValue("InvalidObjectState")INVALID_OBJECT_STATE("InvalidObjectState"),
    @XmlEnumValue("InvalidPart")INVALID_PART("InvalidPart"),
    @XmlEnumValue("InvalidPartOrder")INVALID_PART_ORDER("InvalidPartOrder"),
    @XmlEnumValue("InvalidPayer")INVALID_PAYER("InvalidPayer"),
    @XmlEnumValue("InvalidPolicyDocument")INVALID_POLICY_DOCUMENT("InvalidPolicyDocument"),
    @XmlEnumValue("InvalidRange")INVALID_RANGE("InvalidRange"),
    @XmlEnumValue("InvalidRequest")INVALID_REQUEST("InvalidRequest"),
    @XmlEnumValue("InvalidSecurity")INVALID_SECURITY("InvalidSecurity"),
    @XmlEnumValue("InvalidSOAPRequest")INVALID_SOAP_REQUEST("InvalidSOAPRequest"),
    @XmlEnumValue("InvalidStorageClass")INVALID_STORAGE_CLASS("InvalidStorageClass"),
    @XmlEnumValue("InvalidTargetBucketForLogging")INVALID_TARGET_BUCKET_FOR_LOGGING("InvalidTargetBucketForLogging"),
    @XmlEnumValue("InvalidToken")INVALID_TOKEN("InvalidToken"),
    @XmlEnumValue("InvalidURI")INVALID_URI("InvalidURI"),
    @XmlEnumValue("KeyTooLong")KEY_TOO_LONG("KeyTooLong"),
    @XmlEnumValue("MalformedACLError")MALFORMED_ACL_ERROR("MalformedACLError"),
    @XmlEnumValue("MalformedPOSTRequest")MALFORMED_POST_REQUEST("MalformedPOSTRequest"),
    @XmlEnumValue("MalformedXML")MALFORMED_XML("MalformedXML"),
    @XmlEnumValue("MaxMessageLengthExceeded")MAX_MESSAGE_LENGTH_EXCEEDED("MaxMessageLengthExceeded"),
    @XmlEnumValue("MaxPostPreDataLengthExceededError")MAX_POST_PRE_DATA_LENGTH_EXCEEDED_ERROR("MaxPostPreDataLengthExceededError"),
    @XmlEnumValue("MetadataTooLarge")METADATA_TOO_LARGE("MetadataTooLarge"),
    @XmlEnumValue("MethodNotAllowed")METHOD_NOT_ALLOWED("MethodNotAllowed"),
    @XmlEnumValue("MissingAttachment")MISSING_ATTACHMENT("MissingAttachment"),
    @XmlEnumValue("MissingContentLength")MISSING_CONTENT_LENGTH("MissingContentLength"),
    @XmlEnumValue("MissingRequestBodyError")MISSING_REQUEST_BODY_ERROR("MissingRequestBodyError"),
    @XmlEnumValue("MissingSecurityElement")MISSING_SECURITY_ELEMENT("MissingSecurityElement"),
    @XmlEnumValue("MissingSecurityHeader")MISSING_SECURITY_HEADER("MissingSecurityHeader"),
    @XmlEnumValue("NoLoggingStatusForKey")NO_LOGGING_STATUS_FOR_KEY("NoLoggingStatusForKey"),
    @XmlEnumValue("NoSuchBucket")NO_SUCH_BUCKET("NoSuchBucket"),
    @XmlEnumValue("NoSuchKey")NO_SUCH_KEY("NoSuchKey"),
    @XmlEnumValue("NoSuchLifecycleConfiguration")NO_SUCH_LIFECYCLE_CONFIGURATION("NoSuchLifecycleConfiguration"),
    @XmlEnumValue("NoSuchUpload")NO_SUCH_UPLOAD("NoSuchUpload"),
    @XmlEnumValue("NoSuchVersion")NO_SUCH_VERSION("NoSuchVersion"),
    @XmlEnumValue("NotImplemented")NOT_IMPLEMENTED("NotImplemented"),
    @XmlEnumValue("NotSignedUp")NOT_SIGNED_UP("NotSignedUp"),
    @XmlEnumValue("NoSuchBucketPolicy")NO_SUCH_BUCKET_POLICY("NoSuchBucketPolicy"),
    @XmlEnumValue("OperationAborted")OPERATION_ABORTED("OperationAborted"),
    @XmlEnumValue("PermanentRedirect")PERMANENT_REDIRECT("PermanentRedirect"),
    @XmlEnumValue("PreconditionFailed")PRECONDITION_FAILED("PreconditionFailed"),
    @XmlEnumValue("Redirect")REDIRECT("Redirect"),
    @XmlEnumValue("RestoreAlreadyInProgress")RESTORE_ALREADY_IN_PROGRESS("RestoreAlreadyInProgress"),
    @XmlEnumValue("RequestIsNotMultiPartContent")REQUEST_IS_NOT_MULTI_PART_CONTENT("RequestIsNotMultiPartContent"),
    @XmlEnumValue("RequestTimeout")REQUEST_TIMEOUT("RequestTimeout"),
    @XmlEnumValue("RequestTimeTooSkewed")REQUEST_TIME_TOO_SKEWED("RequestTimeTooSkewed"),
    @XmlEnumValue("RequestTorrentOfBucketError")REQUEST_TORRENT_OF_BUCKET_ERROR("RequestTorrentOfBucketError"),
    @XmlEnumValue("SignatureDoesNotMatch")SIGNATURE_DOES_NOT_MATCH("SignatureDoesNotMatch"),
    @XmlEnumValue("ServiceUnavailable")SERVICE_UNAVAILABLE("ServiceUnavailable"),
    @XmlEnumValue("SlowDown")SLOW_DOWN("SlowDown"),
    @XmlEnumValue("TemporaryRedirect")TEMPORARY_REDIRECT("TemporaryRedirect"),
    @XmlEnumValue("TokenRefreshRequired")TOKEN_REFRESH_REQUIRED("TokenRefreshRequired"),
    @XmlEnumValue("TooManyBuckets")TOO_MANY_BUCKETS("TooManyBuckets"),
    @XmlEnumValue("UnexpectedContent")UNEXPECTED_CONTENT("UnexpectedContent"),
    @XmlEnumValue("UnresolvableGrantByEmailAddress")UNRESOLVABLE_GRANT_BY_EMAIL_ADDRESS("UnresolvableGrantByEmailAddress"),
    @XmlEnumValue("UserKeyMustBeSpecified")USER_KEY_MUST_BE_SPECIFIED("UserKeyMustBeSpecified");

    private final String value;

    ErrorCode(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
