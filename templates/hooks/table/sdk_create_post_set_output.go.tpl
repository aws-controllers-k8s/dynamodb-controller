    if desired.ko.Spec.TimeToLive != nil {
		if err := rm.syncTTL(ctx, &resource{ko}, desired); err != nil {
			return nil, err
		}
	}