package com.jje.bigdata.userProfile.travel.service;

import java.io.Serializable;

import com.jje.bigdata.userProfile.travel.domain.TravelUserProfileInfo;

public interface TravelUserProfileReducer extends Serializable {

	TravelUserProfileInfo process(TravelUserProfileInfo arg0, TravelUserProfileInfo arg1);

}
