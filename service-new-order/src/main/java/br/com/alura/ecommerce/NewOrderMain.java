package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try (var orderDispatcher = new KafkaDispatcher<Order>();
                var emailDispatcher = new KafkaDispatcher<Email>()) {
            for (int i = 0; i < 100; i++) {
                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);
                var order = new Order(userId, orderId, amount);

                var email = new Email("Thank you for your order! we are processing your order", "BODY");
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);
                emailDispatcher.send("ECOMMERCE_SEND_EMAIL", userId, email);
            }
        }
    }
}
