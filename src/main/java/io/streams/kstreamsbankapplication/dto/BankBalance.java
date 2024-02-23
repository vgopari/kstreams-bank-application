package io.streams.kstreamsbankapplication.dto;

import java.time.Instant;

public class BankBalance {

    private Integer count = 0;
    private Integer balance = 0;
    private String time = Instant.ofEpochMilli(0L).toString();

    private BankTransaction recentTransaction;

    public BankBalance() {
    }

    public BankBalance(Integer count, Integer balance, String time, BankTransaction recentTransaction) {
        this.count = count;
        this.balance = balance;
        this.time = time;
        this.recentTransaction = recentTransaction;
    }

    public BankTransaction getRecentTransaction() {
        return recentTransaction;
    }

    public void setRecentTransaction(BankTransaction recentTransaction) {
        this.recentTransaction = recentTransaction;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public Integer getBalance() {
        return balance;
    }

    public void setBalance(Integer balance) {
        this.balance = balance;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BankBalance{");
        sb.append("count=").append(count);
        sb.append(", balance=").append(balance);
        sb.append(", time='").append(time).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
