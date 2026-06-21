import { useState, useEffect, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { Switch } from "@/components/ui/switch";
import { Wifi } from "lucide-react";

export function LanBroadcastSection() {
  const [enabled, setEnabled] = useState(false);
  const [loading, setLoading] = useState(false);

  // Load initial status
  useEffect(() => {
    invoke<boolean>("get_lan_broadcast_status")
      .then(setEnabled)
      .catch(() => {});
  }, []);

  const toggle = useCallback(async (on: boolean) => {
    setLoading(true);
    try {
      if (on) {
        await invoke<string>("start_lan_broadcast");
      } else {
        await invoke<string>("stop_lan_broadcast");
      }
      setEnabled(on);
    } catch (e: any) {
      console.error("LAN broadcast error:", e);
    } finally {
      setLoading(false);
    }
  }, []);

  return (
    <div className="space-y-4 pt-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Wifi className="h-5 w-5 text-green-500" />
          <div>
            <p className="text-sm font-medium">LAN Broadcast</p>
            <p className="text-xs text-muted-foreground">
              Broadcast usage data on port 3345 for local devices
            </p>
          </div>
        </div>
        <Switch
          checked={enabled}
          disabled={loading}
          onCheckedChange={toggle}
        />
      </div>
      {enabled && (
        <p className="text-xs text-green-600 dark:text-green-400">
          Broadcasting on port 3345 · UDP discovery on port 3445
        </p>
      )}
    </div>
  );
}
