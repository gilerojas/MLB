import { getQueueCounts } from "@/lib/db";
import QueueClient from "@/components/QueueClient";

export const dynamic = "force-dynamic";

export default function QueuePage() {
  const counts = getQueueCounts();
  return <QueueClient initialCounts={counts} />;
}
