from __future__ import annotations

from datetime import date

from django.contrib import admin

from djoutbox.models import PendingMessage, SentMessage
from djoutbox.partitions import list_partitions


class PartitionFilter(admin.SimpleListFilter):
    title = "partition"
    parameter_name = "partition"
    _partitions: list[tuple[str, date, date]] | None = None

    def lookups(self, request, model_admin):
        if self._partitions is None:
            self._partitions = list_partitions()
        return [(str(i), p[0]) for i, p in enumerate(self._partitions)]

    def queryset(self, request, queryset):
        if not self.value():
            return queryset
        if self._partitions is None:
            self._partitions = list_partitions()
        idx = int(self.value())
        _, start, end = self._partitions[idx]
        return queryset.filter(created_at__gte=start, created_at__lt=end)


@admin.register(PendingMessage)
class PendingMessageAdmin(admin.ModelAdmin):
    list_display = ("id", "routing_key", "created_at", "send_after", "expiration")
    search_fields = ("routing_key",)
    date_hierarchy = "created_at"
    ordering = ("-created_at",)
    show_full_result_count = False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(SentMessage)
class SentMessageAdmin(admin.ModelAdmin):
    list_display = ("id", "routing_key", "created_at", "send_after", "expiration", "sent_at")
    search_fields = ("routing_key",)
    date_hierarchy = "created_at"
    list_filter = (PartitionFilter,)
    ordering = ("-created_at",)
    show_full_result_count = False

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
