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
			if fIter.IndexStatus != nil {
				fElem.IndexStatus = fIter.IndexStatus
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
		if resp.Table.SSEDescription.Status != nil {
			f.Enabled = aws.Bool(*resp.Table.SSEDescription.Status == "ENABLED")
		} else {
			f.Enabled = aws.Bool(false)
		}
		if resp.Table.SSEDescription.SSEType != nil {
			f.SSEType = resp.Table.SSEDescription.SSEType
		}
		if resp.Table.SSEDescription.KMSMasterKeyArn != nil {
			f.KMSMasterKeyID = resp.Table.SSEDescription.KMSMasterKeyArn
		}
		ko.Spec.SSESpecification = f
	} else {
		ko.Spec.SSESpecification = nil
	}
	if resp.Table.TableClassSummary != nil {
		ko.Spec.TableClass = resp.Table.TableClassSummary.TableClass
	} else {
		ko.Spec.TableClass = aws.String("STANDARD")
	}
	if resp.Table.BillingModeSummary != nil && resp.Table.BillingModeSummary.BillingMode != nil {
		ko.Spec.BillingMode = resp.Table.BillingModeSummary.BillingMode
	} else {
		ko.Spec.BillingMode = aws.String("PROVISIONED")
	}
	if isTableCreating(&resource{ko}) {
		return &resource{ko}, requeueWaitWhileCreating
	}
	if isTableUpdating(&resource{ko}) {
		return &resource{ko}, requeueWaitWhileUpdating
	}
	if isGSICreating(&resource{ko}) {
		ackcondition.SetSynced(
			&resource{ko},
			corev1.ConditionFalse,
			aws.String("Global Secondary Indexes are being created..."),
			nil,
		)
		return &resource{ko}, requeueWaitWhileCreating
	}
	if isGSIUpdating(&resource{ko}) {
		ackcondition.SetSynced(
			&resource{ko},
			corev1.ConditionFalse,
			aws.String("Global Secondary Indexes are being updated..."),
			nil,
		)
		return &resource{ko}, requeueWaitWhileCreating
	}
	if err := rm.setResourceAdditionalFields(ctx, ko); err != nil {
		return nil, err
	}