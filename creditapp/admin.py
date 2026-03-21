from django.contrib import admin

from .models import CreditApplication, FraudResult, Transaction


@admin.register(CreditApplication)
class CreditApplicationAdmin(admin.ModelAdmin):
    list_display = ("applicant_name", "requested_amount", "status", "created_at")
    list_filter = ("status", "employment_status")


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ("applicant", "amount", "merchant", "category", "transaction_time")
    list_filter = ("category", "is_online")


@admin.register(FraudResult)
class FraudResultAdmin(admin.ModelAdmin):
    list_display = ("application", "rule_name", "triggered", "score")
    list_filter = ("triggered", "rule_name")
