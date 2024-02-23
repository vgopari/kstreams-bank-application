package io.streams.kstreamsbankapplication.controller;

import io.streams.kstreamsbankapplication.service.BankProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/bank")
public class BankTransactionController {

    private final BankProducerService bankProducerService;

    @Autowired
    public BankTransactionController(BankProducerService bankProducerService) {
        this.bankProducerService = bankProducerService;
    }

    @GetMapping("/transactions/trigger")
    public void triggerProducer() {
        bankProducerService.produceTransactionRecords();
    }

}
