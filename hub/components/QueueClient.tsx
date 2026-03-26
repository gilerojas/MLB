"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import type { QueueItem } from "@/lib/db";

const STATUS_TABS = ["draft", "approved", "posted", "rejected", "failed"] as const;
type StatusTab = (typeof STATUS_TABS)[number];

const STATUS_COLORS: Record<StatusTab, string> = {
  draft: "bg-yellow-100 text-yellow-800",
  approved: "bg-blue-100 text-blue-800",
  posted: "bg-green-100 text-green-800",
  rejected: "bg-red-100 text-red-800",
  failed: "bg-orange-100 text-orange-800",
};

function CharCounter({ text }: { text: string }) {
  const len = text.length;
  const color =
    len > 280 ? "text-red-600 font-bold" : len > 260 ? "text-orange-500" : "text-gray-400";
  return <span className={`text-xs ${color}`}>{len}/280</span>;
}

function ItemRow({
  item,
  selected,
  onSelect,
}: {
  item: QueueItem;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      onClick={onSelect}
      className={`w-full text-left px-4 py-3 border-b border-gray-100 hover:bg-gray-50 transition-colors ${
        selected ? "bg-blue-50 border-l-4 border-l-[#2c7a7b]" : ""
      }`}
    >
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-medium text-sm text-gray-900 truncate">
            {item.player_name || item.title || "Untitled"}
          </p>
          <p className="text-xs text-gray-500 mt-0.5">
            {item.content_type.replace("_", " ")} · {item.game_date || "—"}
          </p>
        </div>
        <span
          className={`text-xs px-2 py-0.5 rounded-full whitespace-nowrap ${STATUS_COLORS[item.status as StatusTab] || "bg-gray-100 text-gray-700"}`}
        >
          {item.status}
        </span>
      </div>
    </button>
  );
}

export default function QueueClient({
  initialCounts,
}: {
  initialCounts: Record<string, number>;
}) {
  const [activeTab, setActiveTab] = useState<StatusTab>("draft");
  const [items, setItems] = useState<QueueItem[]>([]);
  const [counts, setCounts] = useState(initialCounts);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [tweetText, setTweetText] = useState("");
  const [loading, setLoading] = useState(false);
  const [actionStatus, setActionStatus] = useState<{
    type: "success" | "error";
    msg: string;
  } | null>(null);

  const selectedItem = items.find((i) => i.id === selectedId) ?? null;

  const fetchItems = useCallback(async (status: StatusTab) => {
    const res = await fetch(
      `${process.env.NEXT_PUBLIC_FASTAPI_URL || "http://localhost:8000"}/queue?status=${status}&limit=30`
    );
    if (!res.ok) return;
    const data = await res.json();
    setItems(data.items || []);
  }, []);

  useEffect(() => {
    fetchItems(activeTab);
    setSelectedId(null);
  }, [activeTab, fetchItems]);

  useEffect(() => {
    if (selectedItem) {
      setTweetText(selectedItem.tweet_text || "");
    }
  }, [selectedItem]);

  async function saveTweetText() {
    if (!selectedId) return;
    await fetch(`/api/queue/${selectedId}/tweet-text`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ tweet_text: tweetText }),
    });
  }

  async function handleApprove() {
    if (!selectedId) return;
    setLoading(true);
    setActionStatus(null);
    try {
      await saveTweetText();
      const res = await fetch(`/api/queue/${selectedId}/approve`, {
        method: "POST",
      });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: "success", msg: `Posted! ${data.tweet_url}` });
        await fetchItems(activeTab);
        setSelectedId(null);
        setCounts((c) => ({ ...c, draft: (c.draft || 1) - 1, posted: (c.posted || 0) + 1 }));
      } else {
        setActionStatus({ type: "error", msg: data.error || "Post failed." });
      }
    } finally {
      setLoading(false);
    }
  }

  async function handleReject() {
    if (!selectedId) return;
    setLoading(true);
    setActionStatus(null);
    try {
      const res = await fetch(`/api/queue/${selectedId}/reject`, { method: "POST" });
      const data = await res.json();
      if (res.ok) {
        setActionStatus({ type: "success", msg: "Rejected." });
        await fetchItems(activeTab);
        setSelectedId(null);
        setCounts((c) => ({ ...c, draft: (c.draft || 1) - 1, rejected: (c.rejected || 0) + 1 }));
      } else {
        setActionStatus({ type: "error", msg: data.error || "Reject failed." });
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex h-[calc(100vh-52px)]">
      {/* Left panel — item list */}
      <div className="w-72 border-r border-gray-200 bg-white flex flex-col flex-shrink-0">
        {/* Tabs */}
        <div className="flex overflow-x-auto border-b border-gray-200">
          {STATUS_TABS.map((tab) => (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              className={`px-3 py-2.5 text-xs font-medium whitespace-nowrap capitalize transition-colors ${
                activeTab === tab
                  ? "border-b-2 border-[#2c7a7b] text-[#2c7a7b]"
                  : "text-gray-500 hover:text-gray-700"
              }`}
            >
              {tab}
              {counts[tab] != null && counts[tab] > 0 && (
                <span className="ml-1 bg-gray-100 rounded-full px-1.5 py-0.5 text-gray-600">
                  {counts[tab]}
                </span>
              )}
            </button>
          ))}
        </div>

        {/* Item list */}
        <div className="flex-1 overflow-y-auto">
          {items.length === 0 ? (
            <p className="text-sm text-gray-400 text-center mt-12 px-4">
              No {activeTab} items.
            </p>
          ) : (
            items.map((item) => (
              <ItemRow
                key={item.id}
                item={item}
                selected={selectedId === item.id}
                onSelect={() => setSelectedId(item.id)}
              />
            ))
          )}
        </div>
      </div>

      {/* Right panel — detail + actions */}
      <div className="flex-1 overflow-y-auto p-6">
        {!selectedItem ? (
          <div className="flex items-center justify-center h-full text-gray-400">
            <p>Select a card to review</p>
          </div>
        ) : (
          <div className="max-w-2xl mx-auto space-y-6">
            {/* Header */}
            <div>
              <h2 className="text-xl font-semibold text-gray-900">
                {selectedItem.player_name || selectedItem.title}
              </h2>
              <p className="text-sm text-gray-500 mt-1">
                {selectedItem.content_type.replace("_", " ")} · {selectedItem.game_date}
              </p>
            </div>

            {/* Card image */}
            {selectedItem.image_url && (
              <div className="rounded-lg overflow-hidden shadow border border-gray-200">
                <img
                  src={selectedItem.image_url}
                  alt={selectedItem.title || "Card"}
                  className="w-full object-contain"
                />
              </div>
            )}

            {/* Tweet text editor */}
            <div>
              <div className="flex items-center justify-between mb-1">
                <label className="text-sm font-medium text-gray-700">
                  Tweet text
                </label>
                <CharCounter text={tweetText} />
              </div>
              <textarea
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-[#2c7a7b] resize-none"
                rows={4}
                value={tweetText}
                onChange={(e) => setTweetText(e.target.value)}
                onBlur={saveTweetText}
                disabled={selectedItem.status !== "draft"}
              />
            </div>

            {/* Action buttons (draft only) */}
            {selectedItem.status === "draft" && (
              <div className="flex items-center gap-3">
                <button
                  onClick={handleApprove}
                  disabled={loading || tweetText.length === 0 || tweetText.length > 280}
                  className="bg-[#276749] hover:bg-[#1e5035] text-white font-semibold px-6 py-2.5 rounded-lg text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {loading ? "Posting…" : "Approve & Tweet"}
                </button>
                <button
                  onClick={handleReject}
                  disabled={loading}
                  className="border border-red-300 text-red-600 hover:bg-red-50 font-medium px-5 py-2.5 rounded-lg text-sm transition-colors disabled:opacity-50"
                >
                  Reject
                </button>
              </div>
            )}

            {/* Status feedback */}
            {selectedItem.status === "posted" && selectedItem.twitter_post_id && (
              <div className="bg-green-50 border border-green-200 rounded-lg p-3 text-sm text-green-800">
                Posted · tweet id: {selectedItem.twitter_post_id}
              </div>
            )}
            {selectedItem.status === "failed" && selectedItem.error_message && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3 text-sm text-red-800">
                Failed: {selectedItem.error_message}
              </div>
            )}

            {/* Action result toast */}
            {actionStatus && (
              <div
                className={`rounded-lg p-3 text-sm ${
                  actionStatus.type === "success"
                    ? "bg-green-50 text-green-800 border border-green-200"
                    : "bg-red-50 text-red-800 border border-red-200"
                }`}
              >
                {actionStatus.msg}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
