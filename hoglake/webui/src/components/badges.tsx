import type { SnapshotChange, StatsState } from "../api/types";

export function StatsStateBadge({ state }: { state: StatsState }) {
  return <span className={`badge stats-${state}`}>{state}</span>;
}

export function ChangeBadge({ change }: { change: SnapshotChange }) {
  return (
    <span className="badge change-badge">
      {change.kind}
      {change.object_id !== undefined && (
        <span className="change-object">#{change.object_id}</span>
      )}
    </span>
  );
}
