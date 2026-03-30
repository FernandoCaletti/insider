import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatDate } from "@/lib/utils";
import type { SyncStatus } from "@/lib/types";

function statusColor(status: SyncStatus["status"]): string {
  switch (status) {
    case "success":
      return "bg-success";
    case "error":
      return "bg-destructive";
    case "running":
      return "bg-warning";
    default:
      return "bg-muted-foreground";
  }
}

function calculateNextSync(lastSync: SyncStatus | undefined): string | null {
  if (!lastSync?.finished_at) return null;
  const lastDate = new Date(lastSync.finished_at);
  const nextDate = new Date(lastDate.getTime() + 3 * 24 * 60 * 60 * 1000);
  const now = new Date();
  const diffMs = nextDate.getTime() - now.getTime();
  if (diffMs <= 0) return "em breve";
  const diffDays = Math.ceil(diffMs / (1000 * 60 * 60 * 24));
  if (diffDays === 1) return "em 1 dia";
  return `em ${diffDays} dias`;
}

export function PipelineStatus({
  syncHistory,
}: {
  syncHistory: SyncStatus[];
}) {
  const nextSync = calculateNextSync(syncHistory[0]);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Status do Pipeline</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {syncHistory.length === 0 ? (
          <p className="text-sm text-muted-foreground">
            Nenhum registro de sincronização
          </p>
        ) : (
          <div className="space-y-3">
            {syncHistory.map((sync) => {
              const dateStr = sync.finished_at || sync.started_at;
              const displayDate = formatDate(dateStr.split("T")[0]);
              return (
                <div key={sync.id} className="flex items-center gap-3">
                  <span
                    className={`h-2.5 w-2.5 rounded-full shrink-0 ${statusColor(
                      sync.status
                    )}`}
                  />
                  <span className="text-sm text-muted-foreground">
                    {displayDate}
                  </span>
                  <span className="text-sm font-medium">
                    {sync.documents_processed} docs
                  </span>
                </div>
              );
            })}
          </div>
        )}

        {nextSync && (
          <p className="text-xs text-muted-foreground border-t pt-3">
            Próximo sync {nextSync}
          </p>
        )}
      </CardContent>
    </Card>
  );
}
