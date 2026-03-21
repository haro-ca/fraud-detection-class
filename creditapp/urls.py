from django.urls import path

from . import views

app_name = "creditapp"

urlpatterns = [
    path("", views.index, name="index"),
    path("apply/", views.apply_credit, name="apply"),
    path("status/<int:application_id>/", views.application_status, name="status"),
    path("transactions/", views.transactions_feed, name="transactions"),
    path(
        "api/applications/<int:application_id>/",
        views.api_application_status,
        name="api_application_status",
    ),
    path("api/transactions/", views.api_transactions, name="api_transactions"),
    path("api/fraud-results/", views.api_fraud_results, name="api_fraud_results"),
]
