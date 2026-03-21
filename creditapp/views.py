from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_http_methods

from .models import CreditApplication, FraudResult, Transaction


def index(request):
    applications = CreditApplication.objects.order_by("-created_at")[:20]
    return render(request, "creditapp/index.html", {"applications": applications})


def apply_credit(request):
    if request.method == "POST":
        app = CreditApplication.objects.create(
            applicant_name=request.POST["applicant_name"],
            email=request.POST["email"],
            ssn_last4=request.POST["ssn_last4"],
            annual_income=request.POST["annual_income"],
            requested_amount=request.POST["requested_amount"],
            employment_status=request.POST["employment_status"],
        )
        return redirect("creditapp:status", application_id=app.id)
    return render(request, "creditapp/apply.html")


def application_status(request, application_id):
    application = get_object_or_404(CreditApplication, id=application_id)
    fraud_results = FraudResult.objects.filter(application=application).order_by(
        "-created_at"
    )
    return render(
        request,
        "creditapp/status.html",
        {
            "application": application,
            "fraud_results": fraud_results,
        },
    )


def transactions_feed(request):
    transactions = Transaction.objects.select_related("applicant").order_by(
        "-transaction_time"
    )[:50]
    return render(
        request, "creditapp/transactions.html", {"transactions": transactions}
    )


@require_http_methods(["GET"])
def api_application_status(request, application_id):
    application = get_object_or_404(CreditApplication, id=application_id)
    transactions = Transaction.objects.filter(applicant=application).values(
        "id",
        "amount",
        "merchant",
        "category",
        "transaction_time",
        "location_country",
        "is_online",
    )
    fraud_results = FraudResult.objects.filter(application=application).values(
        "id", "rule_name", "triggered", "score", "details", "created_at"
    )
    return JsonResponse(
        {
            "application": {
                "id": application.id,
                "applicant_name": application.applicant_name,
                "email": application.email,
                "annual_income": str(application.annual_income),
                "requested_amount": str(application.requested_amount),
                "employment_status": application.employment_status,
                "status": application.status,
                "created_at": application.created_at.isoformat(),
            },
            "transactions": list(transactions),
            "fraud_results": list(fraud_results),
        }
    )


@require_http_methods(["GET"])
def api_transactions(request):
    transactions = Transaction.objects.select_related("applicant").order_by(
        "-transaction_time"
    )[:100]
    data = [
        {
            "id": t.id,
            "applicant_id": t.applicant_id,
            "applicant_name": t.applicant.applicant_name,
            "amount": str(t.amount),
            "merchant": t.merchant,
            "category": t.category,
            "transaction_time": t.transaction_time.isoformat(),
            "location_country": t.location_country,
            "is_online": t.is_online,
        }
        for t in transactions
    ]
    return JsonResponse({"transactions": data})


@require_http_methods(["GET"])
def api_fraud_results(request):
    fraud_results = FraudResult.objects.select_related("application").order_by(
        "-created_at"
    )[:100]
    data = [
        {
            "id": f.id,
            "application_id": f.application_id,
            "applicant_name": f.application.applicant_name,
            "rule_name": f.rule_name,
            "triggered": f.triggered,
            "score": str(f.score) if f.score else None,
            "details": f.details,
            "created_at": f.created_at.isoformat(),
        }
        for f in fraud_results
    ]
    return JsonResponse({"fraud_results": data})
