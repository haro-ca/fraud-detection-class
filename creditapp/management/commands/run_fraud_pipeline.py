from django.core.management.base import BaseCommand

from scripts.pipeline import configure_logging, run_pipeline


class Command(BaseCommand):
    help = "Run the fraud detection pipeline"

    def add_arguments(self, parser):
        parser.add_argument(
            "-v",
            "--verbose",
            action="store_true",
            help="Enable verbose (DEBUG) logging",
        )

    def handle(self, *args, **options):
        configure_logging()
        run_pipeline()
        self.stdout.write(self.style.SUCCESS("Fraud pipeline completed successfully"))
