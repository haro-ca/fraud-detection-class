from django.contrib import admin
from django.db.models import Count, Q
from django.utils.html import format_html

from .models import CreditApplication, FraudResult, Transaction


@admin.register(CreditApplication)
class CreditApplicationAdmin(admin.ModelAdmin):
    list_display = (
        "applicant_name",
        "requested_amount",
        "status",
        "created_at",
        "flag_count",
    )
    list_filter = ("status", "employment_status")
    search_fields = ("applicant_name", "email")
    actions = ["approve_applications", "reject_applications"]

    def flag_count(self, obj):
        count = FraudResult.objects.filter(application=obj, triggered=True).count()
        if count > 0:
            return format_html('<span style="color: red;">{}</span>', count)
        return "0"

    flag_count.short_description = "Flags"

    @admin.action(description="Approve selected applications")
    def approve_applications(self, request, queryset):
        queryset.update(status="approved")

    @admin.action(description="Reject selected applications")
    def reject_applications(self, request, queryset):
        queryset.update(status="rejected")


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ("applicant", "amount", "merchant", "category", "transaction_time")
    list_filter = ("category", "is_online")
    search_fields = ("applicant__applicant_name", "merchant")


@admin.register(FraudResult)
class FraudResultAdmin(admin.ModelAdmin):
    list_display = ("application", "rule_name", "triggered", "score", "created_at")
    list_filter = ("triggered", "rule_name")
    search_fields = ("application__applicant_name", "rule_name")

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("application")


class FlaggedApplicationAdmin(admin.ModelAdmin):
    list_display = (
        "applicant_name",
        "requested_amount",
        "status",
        "flag_count",
        "created_at",
    )
    list_filter = ("status", "employment_status")
    search_fields = ("applicant_name", "email")
    actions = ["approve_applications", "reject_applications"]
    change_list_template = "admin/fraud_review_change_list.html"

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        qs = qs.annotate(
            flag_count=Count("fraud_results", filter=Q(fraud_results__triggered=True))
        )
        return qs.filter(flag_count__gt=0)

    def flag_count(self, obj):
        return obj.flag_count

    flag_count.short_description = "Flags"

    @admin.action(description="Approve selected applications")
    def approve_applications(self, request, queryset):
        queryset.update(status="approved")

    @admin.action(description="Reject selected applications")
    def reject_applications(self, request, queryset):
        queryset.update(status="rejected")


admin.site.register(CreditApplication, FlaggedApplicationAdmin)
