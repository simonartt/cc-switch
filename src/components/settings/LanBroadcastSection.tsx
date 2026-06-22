import { useState, useEffect, useCallback } from "react";
import { invoke } from "@tauri-apps/api/core";
import { Switch } from "@/components/ui/switch";
import { Wifi, WifiOff, Copy } from "lucide-react";
import { toast } from "sonner";

export function LanBroadcastSection() {
  const [enabled, setEnabled] = useState(false);
  const [loading, setLoading] = useState(false);
  const [localIp, setLocalIp] = useState("");

  // Load initial status
  useEffect(() => {
    invoke<boolean>("get_lan_broadcast_status")
      .then(setEnabled)
      .catch(() => {});
  }, []);

  // Poll for IP when enabled
  useEffect(() => {
    if (!enabled) {
      setLocalIp("");
      return;
    }
    const poll = async () => {
      try {
        const info: any = await invoke("get_lan_broadcast_info");
        if (info?.localIp) setLocalIp(info.localIp);
      } catch {}
    };
    poll();
    const interval = setInterval(poll, 3000);
    return () => clearInterval(interval);
  }, [enabled]);

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
      toast.error(e?.toString() || "Failed to toggle LAN broadcast");
    } finally {
      setLoading(false);
    }
  }, []);

  const copyToClipboard = useCallback(async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      toast.success("Copied to clipboard");
    } catch {}
  }, []);

  return (
    <div className="space-y-6">
      {/* 主开关 */}
      <div className="flex items-center justify-between p-4 rounded-xl glass-card">
        <div className="flex items-center gap-3">
          {enabled ? (
            <Wifi className="h-6 w-6 text-green-500" />
          ) : (
            <WifiOff className="h-6 w-6 text-muted-foreground" />
          )}
          <div>
            <p className="text-sm font-medium">LAN Broadcast</p>
            <p className="text-xs text-muted-foreground">
              {enabled
                ? "Broadcasting usage data on your local network"
                : "Allow Cardputer & local devices to discover this computer"}
            </p>
          </div>
        </div>
        <Switch checked={enabled} disabled={loading} onCheckedChange={toggle} />
      </div>

      {/* 开启后显示连接信息 */}
      {enabled && (
        <div className="space-y-3 p-4 rounded-xl border border-green-500/30 bg-green-500/5">
          <p className="text-sm font-medium text-green-600 dark:text-green-400">
            Client Connection Info
          </p>

          {/* IP 地址 */}
          <div className="flex items-center justify-between p-3 rounded-lg bg-background/50">
            <div>
              <p className="text-xs text-muted-foreground">Server IP</p>
              <p className="text-sm font-mono">{localIp || "Detecting..."}</p>
            </div>
            {localIp && (
              <button
                onClick={() => copyToClipboard(localIp)}
                className="p-2 hover:bg-muted rounded-md transition-colors"
                title="Copy IP"
              >
                <Copy className="h-4 w-4 text-muted-foreground" />
              </button>
            )}
          </div>

          {/* 端口 */}
          <div className="flex items-center justify-between p-3 rounded-lg bg-background/50">
            <div>
              <p className="text-xs text-muted-foreground">HTTP Port</p>
              <p className="text-sm font-mono">3345</p>
            </div>
            <button
              onClick={() => copyToClipboard("3345")}
              className="p-2 hover:bg-muted rounded-md transition-colors"
              title="Copy Port"
            >
              <Copy className="h-4 w-4 text-muted-foreground" />
            </button>
          </div>

          {/* UDP 发现端口 */}
          <div className="flex items-center justify-between p-3 rounded-lg bg-background/50">
            <div>
              <p className="text-xs text-muted-foreground">
                UDP Discovery Port
              </p>
              <p className="text-sm font-mono">3445</p>
            </div>
            <button
              onClick={() => copyToClipboard("3445")}
              className="p-2 hover:bg-muted rounded-md transition-colors"
              title="Copy Port"
            >
              <Copy className="h-4 w-4 text-muted-foreground" />
            </button>
          </div>

          {/* 完整连接字符串 */}
          {localIp && (
            <div className="p-3 rounded-lg bg-muted/50">
              <p className="text-xs text-muted-foreground mb-1">
                Cardputer Manual Entry
              </p>
              <p className="text-sm font-mono text-green-600 dark:text-green-400">
                {localIp}:3345
              </p>
              <button
                onClick={() => copyToClipboard(`${localIp}:3345`)}
                className="mt-2 text-xs text-primary hover:underline inline-flex items-center gap-1"
              >
                <Copy className="h-3 w-3" /> Copy IP:Port
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
