	// handle in sdkUpdate, to give resource time until it creates
	if desired.ko.Spec.TimeToLive != nil || desired.ko.Spec.ContributorInsights != nil {
		ackcondition.SetSynced(&resource{ko}, corev1.ConditionFalse, nil, nil)
	}
