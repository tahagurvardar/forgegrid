import { useEffect, useState } from "react";
import { api } from "./api";
export function useSnapshot<T>(path: string) {
  const [snapshot, setSnapshot] = useState<{
    path: string;
    data?: T;
    error?: string;
    updated?: Date;
  }>({ path });
  useEffect(() => {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout>;
    let controller: AbortController;
    const load = async () => {
      controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      try {
        const data = await api<T>(path, controller.signal);
        if (!stopped) setSnapshot({ path, data, updated: new Date() });
      } catch (e) {
        if (!stopped)
          setSnapshot((previous) => ({
            ...previous,
            path,
            error: e instanceof Error ? e.message : "Request failed",
          }));
      } finally {
        clearTimeout(timeout);
        if (!stopped) timer = setTimeout(load, 2000);
      }
    };
    setSnapshot({ path });
    void load();
    return () => {
      stopped = true;
      clearTimeout(timer);
      controller?.abort();
    };
  }, [path]);
  return snapshot.path === path ? snapshot : { path };
}
