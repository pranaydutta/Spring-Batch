package com.dutta.batch;

import com.dutta.batch.entity.Customer;
import org.springframework.batch.item.ItemProcessor;


public class CustomerProcessor implements ItemProcessor<Customer, Customer> {


    @Override
    public Customer process(Customer customer) throws Exception {
//        if (customer.getCountry().equals("United States")) {
//            return customer;
//        } else {
//            return null;
//        }


        if(customer.getGender().equals("Female")) {
            int country = Integer.parseInt(customer.getCountry());
        }
                return customer;
    }



}