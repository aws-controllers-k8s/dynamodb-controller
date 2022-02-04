	// Set billing mode in Spec
	if resp.TableDescription.BillingModeSummary != nil && resp.TableDescription.BillingModeSummary.BillingMode != nil {
		ko.Spec.BillingMode = resp.TableDescription.BillingModeSummary.BillingMode
	} else {
		// Default billingMode value is `PROVISIONED`
		ko.Spec.BillingMode = aws.String(svcsdk.BillingModeProvisioned)
	}
	// Set SSESpecification
	if resp.TableDescription.SSEDescription != nil {
		sseSpecification := &svcapitypes.SSESpecification{}
		if resp.TableDescription.SSEDescription.Status != nil {
			switch *resp.TableDescription.SSEDescription.Status {
			case string(svcapitypes.SSEStatus_ENABLED),
				string(svcapitypes.SSEStatus_ENABLING),
				string(svcapitypes.SSEStatus_UPDATING):

				sseSpecification.Enabled = aws.Bool(true)
				if resp.TableDescription.SSEDescription.SSEType != nil {
					sseSpecification.SSEType = resp.TableDescription.SSEDescription.SSEType
				}
			case string(svcapitypes.SSEStatus_DISABLED),
				string(svcapitypes.SSEStatus_DISABLING):
				sseSpecification.Enabled = aws.Bool(false)
			}
		}
		ko.Spec.SSESpecification = sseSpecification
	} else {
		ko.Spec.SSESpecification = &svcapitypes.SSESpecification{
			Enabled: aws.Bool(false),
		}
	}