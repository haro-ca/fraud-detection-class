from django.shortcuts import get_object_or_404, redirect, render

from .models import CreditApplication


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
    return render(request, "creditapp/status.html", {"application": application})
