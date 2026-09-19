import { useCallback, useEffect, useRef, useState } from "react";
import { api, errMsg, type Connection } from "./api";

/** 加载连接列表;错误经 onError 回调上抛(通常传 App.useApp().message.error) */
export function useConnections(onError?: (msg: string) => void) {
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(false);
  const errRef = useRef(onError);
  errRef.current = onError;

  const reload = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    try {
      setConnections(await api.listConnections());
    } catch (e) {
      errRef.current?.(errMsg(e));
    } finally {
      if (!silent) setLoading(false);
    }
  }, []);

  useEffect(() => {
    void reload();
  }, [reload]);

  return { connections, loading, reload };
}
