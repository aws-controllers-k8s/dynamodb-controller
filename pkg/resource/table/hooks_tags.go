// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package table

import (
	"context"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws-controllers-k8s/dynamodb-controller/apis/v1alpha1"
)

// syncTableTags updates a dynamodb table tags.
//
// TODO(hilalymh): move this function to a common utility file. This function can be reused
// to tag GlobalTable resources.
func (rm *resourceManager) syncTableTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableTags")
	defer exit(err)

	from, fromOrder := convertToOrderedACKTags(latest.ko.Spec.Tags)
	to, toOrder := convertToOrderedACKTags(desired.ko.Spec.Tags)
	added, _, removed := ackcompare.GetTagsDifference(from, to)
	toAdd := fromACKTags(added, toOrder)
	toDelete := fromACKTags(removed, fromOrder)

	// Value changes are handled by TagResource alone (GetTagsDifference upsert semantics).

	if len(toDelete) > 0 {
		tagKeys := make([]string, 0, len(toDelete))
		for _, t := range toDelete {
			tagKeys = append(tagKeys, *t.Key)
		}
		_, err = rm.sdkapi.UntagResource(
			ctx,
			&svcsdk.UntagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				TagKeys:     tagKeys,
			},
		)
		rm.metrics.RecordAPICall("GET", "UntagResource", err)
		if err != nil {
			return err
		}
	}

	if len(toAdd) > 0 {
		_, err = rm.sdkapi.TagResource(
			ctx,
			&svcsdk.TagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				Tags:        sdkTagsFromResourceTags(toAdd),
			},
		)
		rm.metrics.RecordAPICall("GET", "TagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// resourceTagsFromSDKTags transforms a *svcsdk.Tag array to a *v1alpha1.Tag array.
func resourceTagsFromSDKTags(svcTags []svcsdktypes.Tag) []*v1alpha1.Tag {
	tags := make([]*v1alpha1.Tag, len(svcTags))
	for i := range svcTags {
		tags[i] = &v1alpha1.Tag{
			Key:   svcTags[i].Key,
			Value: svcTags[i].Value,
		}
	}
	return tags
}

// svcTagsFromResourceTags transforms a *v1alpha1.Tag array to a *svcsdk.Tag array.
func sdkTagsFromResourceTags(rTags []*v1alpha1.Tag) []svcsdktypes.Tag {
	tags := make([]svcsdktypes.Tag, len(rTags))
	for i := range rTags {
		tags[i] = svcsdktypes.Tag{
			Key:   rTags[i].Key,
			Value: rTags[i].Value,
		}
	}
	return tags
}

// getResourceTagsPagesWithContext queries the list of tags of a given resource.
func (rm *resourceManager) getResourceTagsPagesWithContext(ctx context.Context, resourceARN string) ([]*v1alpha1.Tag, error) {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getResourceTagsPagesWithContext")
	defer exit(err)

	tags := []*v1alpha1.Tag{}

	var token *string = nil
	for {
		var listTagsOfResourceOutput *svcsdk.ListTagsOfResourceOutput
		listTagsOfResourceOutput, err = rm.sdkapi.ListTagsOfResource(
			ctx,
			&svcsdk.ListTagsOfResourceInput{
				NextToken:   token,
				ResourceArn: &resourceARN,
			},
		)
		rm.metrics.RecordAPICall("GET", "ListTagsOfResource", err)
		if err != nil {
			return nil, err
		}
		tags = append(tags, resourceTagsFromSDKTags(listTagsOfResourceOutput.Tags)...)
		if listTagsOfResourceOutput.NextToken == nil {
			break
		}
		token = listTagsOfResourceOutput.NextToken
	}
	return tags, nil
}
