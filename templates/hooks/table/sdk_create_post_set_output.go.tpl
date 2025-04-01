    if desired.ko.Spec.TimeToLive != nil {
		if err := rm.syncTTL(ctx, desired, &resource{ko}); err != nil {
			return nil, err
		}
	}

	if desired.ko.Spec.ContributorInsights != nil {
		if err := rm.updateContributorInsights(ctx, desired); err != nil {
			return nil, err
		}
	}
