import django
import polars as pl
from django.db.models import Count  # noqa: E402
from dotenv import load_dotenv

load_dotenv()

django.setup()

from creditapp.models import CreditApplication, FraudResult, Transaction  # noqa: E402
from scripts.pipeline import load_applications, load_transactions, run_all  # noqa: E402


class FraudDetectionServer:
    def get_application_status(self, application_id: int) -> dict:
        """Get the status and details of a credit application."""
        try:
            app = CreditApplication.objects.get(id=application_id)
            transactions = Transaction.objects.filter(applicant=app)
            fraud_results = FraudResult.objects.filter(application=app)

            return {
                "success": True,
                "application": {
                    "id": app.id,
                    "applicant_name": app.applicant_name,
                    "email": app.email,
                    "requested_amount": str(app.requested_amount),
                    "status": app.status,
                    "created_at": app.created_at.isoformat(),
                },
                "transactions_count": transactions.count(),
                "fraud_flags": [
                    {
                        "rule": fr.rule_name,
                        "triggered": fr.triggered,
                        "score": str(fr.score) if fr.score else None,
                        "details": fr.details,
                    }
                    for fr in fraud_results
                ],
            }
        except CreditApplication.DoesNotExist:
            return {
                "success": False,
                "error": f"Application {application_id} not found",
            }

    def get_all_applications(self, status: str | None = None, limit: int = 20) -> dict:
        """Get all credit applications, optionally filtered by status."""
        queryset = CreditApplication.objects.order_by("-created_at")
        if status:
            queryset = queryset.filter(status=status)
        applications = queryset[:limit]

        return {
            "applications": [
                {
                    "id": app.id,
                    "applicant_name": app.applicant_name,
                    "status": app.status,
                    "requested_amount": str(app.requested_amount),
                    "created_at": app.created_at.isoformat(),
                }
                for app in applications
            ],
            "count": len(applications),
        }

    def get_fraud_summary(self) -> dict:
        """Get a summary of fraud detection results."""
        total_apps = CreditApplication.objects.count()
        approved = CreditApplication.objects.filter(status="approved").count()
        rejected = CreditApplication.objects.filter(status="rejected").count()
        pending = CreditApplication.objects.filter(status="pending").count()

        total_flags = FraudResult.objects.filter(triggered=True).count()
        rules_summary = (
            FraudResult.objects.filter(triggered=True)
            .values("rule_name")
            .annotate(count=Count("id"))
        )

        return {
            "applications": {
                "total": total_apps,
                "approved": approved,
                "rejected": rejected,
                "pending": pending,
            },
            "fraud_flags": total_flags,
            "rules_triggered": {r["rule_name"]: r["count"] for r in rules_summary},
        }

    def run_fraud_check(self, application_id: int) -> dict:
        """Run fraud detection for a specific application."""
        try:
            CreditApplication.objects.get(id=application_id)
        except CreditApplication.DoesNotExist:
            return {
                "success": False,
                "error": f"Application {application_id} not found",
            }

        transactions = load_transactions()
        applications = load_applications()

        results = run_all(transactions, applications)
        app_results = results.filter(pl.col("application_id") == application_id)

        if len(app_results) == 0:
            return {
                "success": False,
                "error": f"No fraud results for application {application_id}",
            }

        triggered = [
            row for row in app_results.iter_rows(named=True) if row["triggered"]
        ]
        max_score = max(app_results["score"].to_list()) if len(app_results) > 0 else 0

        return {
            "success": True,
            "application_id": application_id,
            "rules_checked": len(app_results),
            "rules_triggered": len(triggered),
            "max_score": max_score,
            "fraud_results": [
                {
                    "rule": row["rule_name"],
                    "triggered": row["triggered"],
                    "score": row["score"],
                    "details": row["details"],
                }
                for row in app_results.iter_rows(named=True)
            ],
        }

    def list_tools(self) -> list:
        """List available tools."""
        return [
            {
                "name": "get_application_status",
                "description": "Get status and fraud results for a specific application",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "application_id": {
                            "type": "integer",
                            "description": "The application ID",
                        },
                    },
                    "required": ["application_id"],
                },
            },
            {
                "name": "get_all_applications",
                "description": "List all credit applications with optional status filter",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["approved", "rejected", "pending"],
                            "description": "Filter by status",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of applications to return",
                            "default": 20,
                        },
                    },
                },
            },
            {
                "name": "get_fraud_summary",
                "description": "Get a summary of fraud detection results across all applications",
                "inputSchema": {"type": "object", "properties": {}},
            },
            {
                "name": "run_fraud_check",
                "description": "Run fraud detection for a specific application",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "application_id": {
                            "type": "integer",
                            "description": "The application ID",
                        },
                    },
                    "required": ["application_id"],
                },
            },
        ]


server = FraudDetectionServer()
