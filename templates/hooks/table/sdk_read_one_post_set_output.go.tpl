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