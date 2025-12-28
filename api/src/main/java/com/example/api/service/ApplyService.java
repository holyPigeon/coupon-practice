package com.example.api.service;

import com.example.api.domain.Coupon;
import com.example.api.producer.CouponCreateProducer;
import com.example.api.repository.AppliedUserRepository;
import com.example.api.repository.CouponCountRepository;
import com.example.api.repository.CouponRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ApplyService {

    private final CouponCountRepository couponCountRepository;
    private final CouponCreateProducer couponCreateProducer;
    private final AppliedUserRepository appliedUserRepository;

    public void apply(Long userId) {
        Long apply = appliedUserRepository.add(userId);
        if (apply != 1) {
            return;
        }

        Long count = couponCountRepository.increment();

        if (count > 100) {
            return;
        }

//        Coupon coupon = new Coupon(userId);
//        couponRepository.save(coupon);
        couponCreateProducer.create(userId);
    }
}
