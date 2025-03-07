	if resp.Table.GlobalSecondaryIndexes != nil {
		f := []*svcapitypes.GlobalSecondaryIndexDescription{}
		for _, fIter := range resp.Table.GlobalSecondaryIndexes {
			fElem := &svcapitypes.GlobalSecondaryIndexDescription{}
			if fIter.IndexName != nil {
				fElem.IndexName = fIter.IndexName
			}
			if fIter.IndexArn != nil {
				fElem.IndexARN = fIter.IndexArn
			}
			if fIter.ItemCount != nil {
				fElem.ItemCount = fIter.ItemCount
			}
			if fIter.IndexStatus != "" {
				fElem.IndexStatus = aws.String(string(fIter.IndexStatus))
			}
			if fIter.IndexSizeBytes != nil {
				fElem.IndexSizeBytes = fIter.IndexSizeBytes
			}
			if fIter.Backfilling != nil {
				fElem.Backfilling = fIter.Backfilling
			}
			f = append(f, fElem)
		}
		ko.Status.GlobalSecondaryIndexesDescriptions = f
	} else {
		ko.Status.GlobalSecondaryIndexesDescriptions = nil
	}
	if resp.Table.SSEDescription != nil {
		f := &svcapitypes.SSESpecification{}
		if resp.Table.SSEDescription.Status != "" {
			f.Enabled = aws.Bool(resp.Table.SSEDescription.Status == svcsdktypes.SSEStatusEnabled)
		} else {
			f.Enabled = aws.Bool(false)
		}
		if resp.Table.SSEDescription.SSEType != "" {
			f.SSEType = aws.String(string(resp.Table.SSEDescription.SSEType))
		}
		if resp.Table.SSEDescription.KMSMasterKeyArn != nil {
			f.KMSMasterKeyID = resp.Table.SSEDescription.KMSMasterKeyArn
		}
		ko.Spec.SSESpecification = f
	} else {
		ko.Spec.SSESpecification = nil
	}
	if resp.Table.TableClassSummary != nil {
		ko.Spec.TableClass = aws.String(string(resp.Table.TableClassSummary.TableClass))
	} else {
		ko.Spec.TableClass = aws.String("STANDARD")
	}
	if resp.Table.BillingModeSummary != nil && resp.Table.BillingModeSummary.BillingMode != "" {
		ko.Spec.BillingMode = aws.String(string(resp.Table.BillingModeSummary.BillingMode))
	} else {
		ko.Spec.BillingMode = aws.String("PROVISIONED")
	}
	if resp.Table.Replicas != nil {
		f12 := []*svcapitypes.ReplicaDescription{}
		for _, f12iter := range resp.Table.Replicas {
			f12elem := &svcapitypes.ReplicaDescription{}
			if f12iter.RegionName != nil {
				f12elem.RegionName = f12iter.RegionName
			}
			if f12iter.ReplicaStatus != "" {
				f12elem.ReplicaStatus = aws.String(string(f12iter.ReplicaStatus))
			} else {
				f12elem.ReplicaStatus = aws.String("Unknown")
			}
			if f12iter.ReplicaStatusDescription != nil {
				f12elem.ReplicaStatusDescription = f12iter.ReplicaStatusDescription
			} else {
				f12elem.ReplicaStatusDescription = aws.String("")
			}
			if f12iter.ReplicaStatusPercentProgress != nil {
				f12elem.ReplicaStatusPercentProgress = f12iter.ReplicaStatusPercentProgress
			} else {
				f12elem.ReplicaStatusPercentProgress = aws.String("0")
			}
			if f12iter.ReplicaInaccessibleDateTime != nil {
				f12elem.ReplicaInaccessibleDateTime = &metav1.Time{Time: *f12iter.ReplicaInaccessibleDateTime}
			} else {
				f12elem.ReplicaInaccessibleDateTime = nil
			}
			if f12iter.ReplicaTableClassSummary != nil && f12iter.ReplicaTableClassSummary.TableClass != "" {
				f12elem.ReplicaTableClassSummary.TableClass = aws.String(string(f12iter.ReplicaTableClassSummary.TableClass))
				f12elem.ReplicaTableClassSummary.LastUpdateDateTime = &metav1.Time{Time: *f12iter.ReplicaTableClassSummary.LastUpdateDateTime}
			} else {
				f12elem.ReplicaTableClassSummary.TableClass = aws.String("STANDARD")
			}
			f12 = append(f12, f12elem)
		}
		ko.Status.ReplicasDescriptions = f12
	} else {
		ko.Status.ReplicasDescriptions = nil
	}
	if isTableCreating(&resource{ko}) {
		return &resource{ko}, requeueWaitWhileCreating
	}
	if isTableUpdating(&resource{ko}) {
		return &resource{ko}, requeueWaitWhileUpdating
	}
	if !canUpdateTableGSIs(&resource{ko}) {
		return &resource{ko}, requeueWaitGSIReady
	}
	if err := rm.setResourceAdditionalFields(ctx, ko); err != nil {
		return nil, err
	}