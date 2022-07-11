    if desired.ko.Spec.TimeToLive != nil {
		if err := rm.syncTTL(ctx, desired, &resource{ko}); err != nil {
			return nil, err
		}
	}