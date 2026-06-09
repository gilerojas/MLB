"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { getApiBase } from "@/lib/api";

type SummaryRow = {
  posts: number;
  impressions: number;
  likes: number;
  replies: number;
  reposts: number;
  quote_tweets: number;
  bookmarks: number;
  profile_visits: number;
  follows: number;
  engagement_rate: number;
  bookmark_rate: number;
  reply_rate: number;
  repost_rate: number;
  follows_per_1000_impressions: number;
  bookmarks_per_1000_impressions: number;
  replies_per_1000_impressions: number;
  reposts_per_1000_impressions: number;
  content_pillar?: string;
  hook_type?: string;
  content_type?: string;
};

type TopPost = {
  queue_item_id: number;
  title: string | null;
  player_name: string | null;
  tweet_text: string | null;
  content_type: string | null;
  content_pillar: string | null;
  hook_type: string | null;
  intended_kpi: string | null;
  impressions: number;
  replies: number;
  reposts: number;
  bookmarks: number;
  follows: number;
  engagement_rate: number;
};

type MissingMetric = {
  queue_item_id: number;
  title: string | null;
  player_name: string | null;
  content_type: string | null;
  content_pillar: string | null;
  hook_type: string | null;
  intended_kpi: string | null;
  priority_score: number | null;
  posted_at: string | null;
};

type GrowthPayload = {
  days: number;
  summary: SummaryRow;
  by_pillar: SummaryRow[];
  by_hook_type: SummaryRow[];
  by_content_type: SummaryRow[];
  top_posts: Record<string, TopPost[]>;
  missing_metrics: MissingMetric[];
  queue_health: {
    queue_drafts: number;
    queue_posted: number;
    queue_rejected: number;
    queue_failed: number;
    failed_post_rate: number;
    posted_without_metrics: number;
  };
};

const EMPTY_SUMMARY: SummaryRow = {
  posts: 0,
  impressions: 0,
  likes: 0,
  replies: 0,
  reposts: 0,
  quote_tweets: 0,
  bookmarks: 0,
  profile_visits: 0,
  follows: 0,
  engagement_rate: 0,
  bookmark_rate: 0,
  reply_rate: 0,
  repost_rate: 0,
  follows_per_1000_impressions: 0,
  bookmarks_per_1000_impressions: 0,
  replies_per_1000_impressions: 0,
  reposts_per_1000_impressions: 0,
};

function fmtInt(value: number | null | undefined) {
  return Math.round(Number(value || 0)).toLocaleString();
}

function fmtRate(value: number | null | undefined) {
  return `${(Number(value || 0) * 100).toFixed(2)}%`;
}

function fmtPer1k(value: number | null | undefined) {
  return Number(value || 0).toFixed(2);
}

function titleCase(value: string | null | undefined) {
  if (!value) return "Unknown";
  return value.replace(/_/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function shortDate(value: string | null | undefined) {
  if (!value) return "-";
  return value.slice(0, 10);
}

function StatCard({ label, value, tone = "default" }: { label: string; value: string; tone?: "default" | "accent" | "warn" }) {
  const toneClass =
    tone === "accent"
      ? "text-accent"
      : tone === "warn"
        ? "text-warning"
        : "text-foreground";
  return (
    <div className="border border-outline-variant/40 bg-surface px-3 py-3">
      <p className="text-[10px] font-mono uppercase text-dim tracking-wide">{label}</p>
      <p className={`mt-1 text-2xl font-headline font-bold tabular-nums ${toneClass}`}>{value}</p>
    </div>
  );
}

function GroupTable({ title, labelKey, rows }: { title: string; labelKey: "content_pillar" | "hook_type" | "content_type"; rows: SummaryRow[] }) {
  return (
    <section className="border border-outline-variant/40 bg-surface">
      <div className="border-b border-outline-variant/30 px-4 py-3">
        <h2 className="text-sm font-headline font-bold uppercase text-foreground">{title}</h2>
      </div>
      {rows.length === 0 ? (
        <p className="px-4 py-5 text-sm text-dim">No post metrics entered yet.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-[10px] font-mono uppercase text-dim border-b border-outline-variant/30">
              <tr>
                <th className="text-left px-4 py-2">Group</th>
                <th className="text-right px-3 py-2">Posts</th>
                <th className="text-right px-3 py-2">Impr.</th>
                <th className="text-right px-3 py-2">Bkm/1k</th>
                <th className="text-right px-3 py-2">Rep/1k</th>
                <th className="text-right px-3 py-2">Rpl/1k</th>
                <th className="text-right px-4 py-2">Follows</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${labelKey}-${row[labelKey] || "unknown"}`} className="border-b border-outline-variant/20 last:border-0">
                  <td className="px-4 py-2 font-mono text-foreground">{titleCase(row[labelKey])}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fmtInt(row.posts)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fmtInt(row.impressions)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fmtPer1k(row.bookmarks_per_1000_impressions)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fmtPer1k(row.reposts_per_1000_impressions)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{fmtPer1k(row.replies_per_1000_impressions)}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{fmtInt(row.follows)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function TopPosts({ posts }: { posts: TopPost[] }) {
  return (
    <section className="border border-outline-variant/40 bg-surface">
      <div className="border-b border-outline-variant/30 px-4 py-3">
        <h2 className="text-sm font-headline font-bold uppercase text-foreground">Top posts</h2>
      </div>
      {posts.length === 0 ? (
        <p className="px-4 py-5 text-sm text-dim">No tracked posts yet.</p>
      ) : (
        <div className="divide-y divide-outline-variant/20">
          {posts.map((post) => (
            <div key={post.queue_item_id} className="px-4 py-3 grid gap-2 md:grid-cols-[1fr_auto] md:items-center">
              <div className="min-w-0">
                <p className="text-sm font-headline font-bold text-foreground truncate">
                  {post.player_name || post.title || `Queue #${post.queue_item_id}`}
                </p>
                <p className="text-xs text-dim truncate">{post.tweet_text || titleCase(post.content_type)}</p>
                <p className="mt-1 text-[10px] font-mono uppercase text-muted">
                  {titleCase(post.content_pillar)} / Primary KPI: {titleCase(post.intended_kpi)}
                </p>
              </div>
              <div className="grid grid-cols-4 gap-2 text-right text-xs font-mono">
                <span>Bkm {fmtInt(post.bookmarks)}</span>
                <span>Rpl {fmtInt(post.replies)}</span>
                <span>Rp {fmtInt(post.reposts)}</span>
                <span>Eng {fmtRate(post.engagement_rate)}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

function MissingMetrics({ rows }: { rows: MissingMetric[] }) {
  return (
    <section className="border border-outline-variant/40 bg-surface">
      <div className="border-b border-outline-variant/30 px-4 py-3 flex items-center justify-between gap-3">
        <h2 className="text-sm font-headline font-bold uppercase text-foreground">Posted without metrics</h2>
        <Link href="/queue" className="text-xs font-mono uppercase text-accent hover:underline">Open queue</Link>
      </div>
      {rows.length === 0 ? (
        <p className="px-4 py-5 text-sm text-dim">Every recent posted item has metrics.</p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="text-[10px] font-mono uppercase text-dim border-b border-outline-variant/30">
              <tr>
                <th className="text-left px-4 py-2">Item</th>
                <th className="text-left px-3 py-2">Pillar</th>
                <th className="text-left px-3 py-2">Primary KPI</th>
                <th className="text-right px-3 py-2">Priority</th>
                <th className="text-right px-4 py-2">Posted</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 12).map((row) => (
                <tr key={row.queue_item_id} className="border-b border-outline-variant/20 last:border-0">
                  <td className="px-4 py-2">
                    <p className="font-mono text-foreground">#{row.queue_item_id} {row.player_name || row.title || titleCase(row.content_type)}</p>
                    <p className="text-xs text-dim">{titleCase(row.content_type)}</p>
                  </td>
                  <td className="px-3 py-2">{titleCase(row.content_pillar)}</td>
                  <td className="px-3 py-2">{titleCase(row.intended_kpi)}</td>
                  <td className="px-3 py-2 text-right tabular-nums">{row.priority_score ?? "-"}</td>
                  <td className="px-4 py-2 text-right tabular-nums">{shortDate(row.posted_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

export default function GrowthPage() {
  const api = getApiBase();
  const [days, setDays] = useState(30);
  const [data, setData] = useState<GrowthPayload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchGrowth = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`${api}/analytics/growth-summary?days=${days}`, { cache: "no-store" });
      const body = await res.json();
      if (!res.ok) throw new Error(typeof body.detail === "string" ? body.detail : "Growth summary failed.");
      setData(body as GrowthPayload);
    } catch (e) {
      setError(String(e));
      setData(null);
    } finally {
      setLoading(false);
    }
  }, [api, days]);

  useEffect(() => {
    void fetchGrowth();
  }, [fetchGrowth]);

  const summary = data?.summary || EMPTY_SUMMARY;
  const topPosts = useMemo(() => data?.top_posts?.bookmarks || [], [data]);

  return (
    <div className="p-4 lg:p-6 max-w-[1800px] mx-auto w-full space-y-5">
      <div className="flex flex-col lg:flex-row lg:items-end lg:justify-between gap-3 border-b border-outline pb-4">
        <div>
          <p className="text-xs font-mono uppercase tracking-widest text-accent">Console / Growth</p>
          <h1 className="text-2xl font-headline font-bold text-foreground mt-1">Growth cockpit</h1>
          <p className="text-sm text-dim mt-1">Last {days} days. Manual metrics. Primary KPI is the intended goal, not the only metric that matters.</p>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-xs font-mono uppercase text-dim">Window</span>
          <select
            value={days}
            onChange={(e) => setDays(Number(e.target.value))}
            className="border border-outline-variant bg-surface text-foreground px-2 py-1.5 text-sm font-mono"
          >
            <option value={7}>7 days</option>
            <option value={30}>30 days</option>
            <option value={60}>60 days</option>
            <option value={90}>90 days</option>
          </select>
        </div>
      </div>

      {error && <div className="border border-danger-border bg-danger-bg px-4 py-3 text-sm text-danger font-mono">{error}</div>}
      {loading && <div className="border border-outline-variant/40 bg-surface px-4 py-6 text-sm text-dim">Loading growth summary...</div>}

      {!loading && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-4 xl:grid-cols-8 gap-3">
            <StatCard label="Tracked" value={fmtInt(summary.posts)} tone="accent" />
            <StatCard label="Missing" value={fmtInt(data?.queue_health?.posted_without_metrics || 0)} tone="warn" />
            <StatCard label="Impressions" value={fmtInt(summary.impressions)} />
            <StatCard label="Bookmarks" value={fmtInt(summary.bookmarks)} />
            <StatCard label="Replies" value={fmtInt(summary.replies)} />
            <StatCard label="Reposts" value={fmtInt(summary.reposts)} />
            <StatCard label="Follows" value={fmtInt(summary.follows)} />
            <StatCard label="Eng. rate" value={fmtRate(summary.engagement_rate)} />
          </div>

          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <StatCard label="Bookmarks / 1k" value={fmtPer1k(summary.bookmarks_per_1000_impressions)} />
            <StatCard label="Replies / 1k" value={fmtPer1k(summary.replies_per_1000_impressions)} />
            <StatCard label="Reposts / 1k" value={fmtPer1k(summary.reposts_per_1000_impressions)} />
            <StatCard label="Follows / 1k" value={fmtPer1k(summary.follows_per_1000_impressions)} />
          </div>

          <div className="grid xl:grid-cols-3 gap-4">
            <GroupTable title="Performance by pillar" labelKey="content_pillar" rows={data?.by_pillar || []} />
            <GroupTable title="Performance by hook" labelKey="hook_type" rows={data?.by_hook_type || []} />
            <GroupTable title="Performance by type" labelKey="content_type" rows={data?.by_content_type || []} />
          </div>

          <div className="grid xl:grid-cols-2 gap-4">
            <TopPosts posts={topPosts} />
            <MissingMetrics rows={data?.missing_metrics || []} />
          </div>
        </>
      )}
    </div>
  );
}
