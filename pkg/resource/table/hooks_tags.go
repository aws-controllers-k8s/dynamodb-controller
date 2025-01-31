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

	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	ackutil "github.com/aws-controllers-k8s/runtime/pkg/util"
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
	latest *resource,
	desired *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTableTags")
	defer exit(err)

	added, removed := computeTagsDelta(latest.ko.Spec.Tags, desired.ko.Spec.Tags)

	// There are no API calls to update an existing tag. To update a tag we will have to first
	// delete it and then recreate it with the new value.

	if len(removed) > 0 {
		_, err = rm.sdkapi.UntagResource(
			ctx,
			&svcsdk.UntagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				TagKeys:     removed,
			},
		)
		rm.metrics.RecordAPICall("GET", "UntagResource", err)
		if err != nil {
			return err
		}
	}

	if len(added) > 0 {
		_, err = rm.sdkapi.TagResource(
			ctx,
			&svcsdk.TagResourceInput{
				ResourceArn: (*string)(latest.ko.Status.ACKResourceMetadata.ARN),
				Tags:        sdkTagsFromResourceTags(added),
			},
		)
		rm.metrics.RecordAPICall("GET", "UntagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// equalTags returns true if two Tag arrays are equal regardless of the order
// of their elements.
func equalTags(
	a []*v1alpha1.Tag,
	b []*v1alpha1.Tag,
) bool {
	added, removed := computeTagsDelta(a, b)
	return len(added) == 0 && len(removed) == 0
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

// computeTagsDelta compares two Tag arrays and return three different list
// containing the added, updated and removed tags.
// The removed tags only contains the Key of tags
func computeTagsDelta(
	a []*v1alpha1.Tag,
	b []*v1alpha1.Tag,
) (added []*v1alpha1.Tag, removed []string) {
	var visitedIndexes []string
mainLoop:
	for _, aElement := range b {
		visitedIndexes = append(visitedIndexes, *aElement.Key)
		for _, bElement := range a {
			if equalStrings(aElement.Key, bElement.Key) {
				if !equalStrings(aElement.Value, bElement.Value) {
					added = append(added, bElement)
				}
				continue mainLoop
			}
		}
		removed = append(removed, *aElement.Key)
	}
	for _, bElement := range a {
		if !ackutil.InStrings(*bElement.Key, visitedIndexes) {
			added = append(added, bElement)
		}
	}
	return added, removed
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
