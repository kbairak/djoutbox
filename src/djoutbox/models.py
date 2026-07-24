from django.db import models


class PendingMessage(models.Model):
    id = models.BigAutoField(primary_key=True)
    routing_key = models.TextField()
    body = models.BinaryField()
    tracking_ids = models.JSONField()
    created_at = models.DateTimeField()
    send_after = models.DateTimeField()
    expiration = models.DurationField(null=True)

    class Meta:
        db_table = "djoutbox_pending"
        managed = False


class SentMessage(models.Model):
    id = models.BigIntegerField(primary_key=True)
    routing_key = models.TextField()
    body = models.BinaryField()
    tracking_ids = models.JSONField()
    created_at = models.DateTimeField()
    send_after = models.DateTimeField()
    expiration = models.DurationField(null=True)
    sent_at = models.DateTimeField()

    class Meta:
        db_table = "djoutbox_sent"
        managed = False
