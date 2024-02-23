package io.streams.kstreamsbankapplication.dto;

import java.util.Objects;

public class BankTransaction {

    private String name;
    private Integer amount;
    private String timestamp;
    private BankTransactionStatus status = BankTransactionStatus.CREATED;
    private TransactionType transactionType;

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public BankTransactionStatus getStatus() {
        return status;
    }

    public void setStatus(BankTransactionStatus status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BankTransaction that = (BankTransaction) o;
        return Objects.equals(status, that.status) &&
                Objects.equals(name, that.name) &&
                Objects.equals(amount, that.amount) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(transactionType, that.transactionType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, amount, timestamp, status, transactionType);
    }

    @Override
    public String toString() {
        return "{" +
                "\"name\":\"" + name + '\"' +
                ", \"amount\":" + amount +
                ", \"timestamp\":\"" + timestamp + '\"' +
                ", \"status\":\"" + status + '\"' +
                ", \"transactionType\":\"" + transactionType + '\"' +
                '}';
    }

}
