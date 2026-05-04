import Link from 'next/link';

export default function HomePage() {
  return (
    <main className="flex flex-1 flex-col justify-center text-center px-4 py-16">
      <h1 className="mb-4 text-4xl font-bold tracking-tight">Orchestra</h1>
      <p className="mb-2 max-w-2xl mx-auto text-fd-muted-foreground">
        Translate Azure Data Factory pipelines to Databricks Lakeflow Jobs via
        Declarative Automation Bundles.
      </p>
      <p className="mb-8 max-w-2xl mx-auto text-fd-muted-foreground">
        Deterministic translators for known activity types, agentic fallback
        for everything else, and a Databricks Asset Bundle ready to deploy.
      </p>
      <div className="flex justify-center gap-3">
        <Link
          href="/docs"
          className="inline-flex items-center rounded-md bg-fd-primary px-4 py-2 text-sm font-medium text-fd-primary-foreground hover:opacity-90"
        >
          Read the docs
        </Link>
        <Link
          href="https://github.com/ghanse/orchestra"
          className="inline-flex items-center rounded-md border border-fd-border px-4 py-2 text-sm font-medium hover:bg-fd-muted"
        >
          View on GitHub
        </Link>
      </div>
    </main>
  );
}
