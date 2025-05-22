package com.jpmc.midascore.foundation;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-core-group",
            containerFactory = "transactionKafkaListenerContainerFactory"
    )
    public void listen(Transaction transaction) {
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            System.out.println("Invalid sender or recipient. Transaction discarded.");
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("Insufficient balance. Transaction discarded.");
            return;
        }

        // Adjust balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save updated users
        userRepository.save(sender);
        userRepository.save(recipient);

        // Record the transaction
        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(transaction.getAmount());
        record.setTimestamp(LocalDateTime.now());

        transactionRecordRepository.save(record);

        System.out.println("Transaction processed and recorded: " + transaction);

    }
}
