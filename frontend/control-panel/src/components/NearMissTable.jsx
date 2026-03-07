import React, { useMemo, useState, useRef, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  AlertCircle, ExternalLink, ShieldAlert, ZapOff, Clock, Radar,
  ArrowUpCircle, ArrowDownCircle, ChevronUp, ChevronDown,
  ArrowUpDown, Play, MoveRight, LineChart
} from 'lucide-react';
import { cn, formatNumber } from '../lib/utils';
import { getNearMissSignals, getDailyFocus, promoteNearMiss } from '../services/api';
import { toast } from 'sonner';
import SignalMetricsTooltip from './SignalMetricsTooltip';

const SortableHeader = ({ label, sortKey, currentSort, onSort }) => {
  const isActive = currentSort.key === sortKey;
  return (
    <th
      className="p-2 cursor-pointer hover:bg-foreground/5 transition-colors group select-none"
      onClick={() => onSort(sortKey)}
    >
      <div className="flex items-center gap-1">
        {label}
        <div className={cn(
          "transition-opacity",
          isActive ? "opacity-100" : "opacity-0 group-hover:opacity-40"
        )}>
          {isActive ? (
            currentSort.direction === 'asc' ? <ChevronUp size={10} /> : <ChevronDown size={10} />
          ) : <ArrowUpDown size={10} />}
        </div>
      </div>
    </th>
  );
};

const NearMissTable = ({ playSound }) => {
  const queryClient = useQueryClient();
  const [logSort, setLogSort] = useState({ key: 'timestamp', direction: 'desc' });

  // 1. Fetch Logged Audit Records
  const { data: signals, isLoading: logsLoading } = useQuery({
    queryKey: ['near_miss_signals'],
    queryFn: getNearMissSignals,
    refetchInterval: 5000
  });

  // 2. Fetch Daily Focus
  const { data: focus, isLoading: focusLoading } = useQuery({
    queryKey: ['focus_proximity'],
    queryFn: getDailyFocus,
    refetchInterval: 3000
  });

  // 3. Promotion Mutation
  const promoteMutation = useMutation({
    mutationFn: promoteNearMiss,
    onSuccess: (data) => {
      toast.success(data.message);
      queryClient.invalidateQueries({ queryKey: ['near_miss_signals'] });
      queryClient.invalidateQueries({ queryKey: ['signals'] });
    },
    onError: (err) => {
      toast.error(err.response?.data?.detail || "Promotion failed");
    }
  });

  const getOracleBadge = (status) => {
    switch (status) {
      case 'UP_SNIPER': return <span className="bg-green-500 text-white px-1.5 py-0.5 rounded text-[9px] font-extrabold tracking-tighter">UP</span>;
      case 'DOWN_SNIPER': return <span className="bg-red-500 text-white px-1.5 py-0.5 rounded text-[9px] font-extrabold tracking-tighter">DW</span>;
      case 'FILTERED': return <span className="bg-yellow-500/10 text-yellow-600 border border-yellow-500/20 px-1.5 py-0.5 rounded text-[9px] font-bold">FILT</span>;
      default: return <span className="bg-muted text-muted-foreground px-1.5 py-0.5 rounded text-[9px] font-bold opacity-50">{status || 'N/A'}</span>;
    }
  };

  const getGoogleFinanceLink = (symbol) => `https://www.google.com/finance/quote/${symbol}:NSE`;


  const getValue = (sig, key) => {
    if (sig[key] !== undefined) return sig[key];
    const metrics = sig.metrics || {};
    if (metrics[key] !== undefined) return metrics[key];
    return undefined;
  };

  const sortedLogs = useMemo(() => {
    if (!signals) return [];
    return [...signals].sort((a, b) => {
      let valA = getValue(a, logSort.key);
      let valB = getValue(b, logSort.key);
      if (valA === undefined) valA = -Infinity;
      if (valB === undefined) valB = -Infinity;
      if (typeof valA === 'string') valA = valA.toLowerCase();
      if (typeof valB === 'string') valB = valB.toLowerCase();
      if (valA < valB) return logSort.direction === 'asc' ? -1 : 1;
      if (valA > valB) return logSort.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [signals, logSort]);

  // 4. Proximity Audio Alert Logic
  const [hasInitializedProximity, setHasInitializedProximity] = useState(false);
  const seenProximityRef = useRef(new Set());

  const proximityWatchlist = useMemo(() => {
    if (!focus || !Array.isArray(focus)) return [];

    const currentProximity = focus.map(stock => {
      const livePrice = stock.live_price || 0;
      const orbHigh = stock.orb_high || 0;
      const orbLow = stock.orb_low || 0;
      const oracle = stock.oracle_status || 'NEUTRAL';

      if (!livePrice || !orbHigh || !orbLow) return null;

      const distHigh = ((orbHigh - livePrice) / orbHigh) * 100;
      const distLow = ((livePrice - orbLow) / orbLow) * 100;

      if (livePrice > (orbHigh + 0.05) || livePrice < (orbLow - 0.05)) return null;

      const threshold = 0.8;
      const isNearHigh = distHigh > 0 && distHigh <= threshold;
      const isNearLow = distLow > 0 && distLow <= threshold;

      if (!isNearHigh && !isNearLow) return null;

      return {
        ...stock,
        near_type: isNearHigh ? 'BREAKOUT' : 'BREAKDOWN',
        distance: isNearHigh ? distHigh : distLow,
        target_price: isNearHigh ? orbHigh : orbLow,
        bias_match: (isNearHigh && oracle === 'UP_SNIPER') || (isNearLow && oracle === 'DOWN_SNIPER'),
        side: isNearHigh ? 'LONG' : 'SHORT'
      };
    }).filter(Boolean).sort((a, b) => a.distance - b.distance);

    return currentProximity;
  }, [focus]);

  // Effect to trigger sound
  useEffect(() => {
    if (proximityWatchlist.length > 0) {
      if (!hasInitializedProximity) {
        proximityWatchlist.forEach(s => seenProximityRef.current.add(s.symbol));
        setHasInitializedProximity(true);
        return;
      }

      let foundNew = false;
      proximityWatchlist.forEach(s => {
        if (!seenProximityRef.current.has(s.symbol)) {
          seenProximityRef.current.add(s.symbol);
          foundNew = true;
        }
      });

      if (foundNew) {
        playSound('radar');
      }
    }
  }, [proximityWatchlist, hasInitializedProximity, playSound]);

  if (logsLoading || focusLoading) return (
    <div className="glass rounded-2xl border border-border p-8 text-center animate-pulse text-muted-foreground uppercase text-[10px] font-black tracking-widest">
      Scanning Market for Opportunities...
    </div>
  );

  const handleLogSort = (key) => {
    setLogSort(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }));
  };

  return (
    <div className="space-y-6 mt-6">
      {/* 1. Proximity Watchlist (Pre-Breakout) */}
      {proximityWatchlist.length > 0 && (
        <section className="glass rounded-2xl border border-border overflow-hidden flex flex-col animate-in fade-in slide-in-from-bottom-8 duration-500">
          <div className="p-3 border-b border-border bg-primary/[0.03] flex items-center justify-between shrink-0">
            <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-primary">
              <Radar className="w-4 h-4 animate-pulse" /> Live Proximity Watchlist (Getting Ready)
            </h2>
            <span className="text-[10px] font-bold bg-primary/10 px-2 py-1 rounded text-primary uppercase tracking-widest">
              {proximityWatchlist.length} IN RANGE
            </span>
          </div>
          <div className="overflow-auto max-h-[400px]">
            <table className="w-full text-[10px] whitespace-nowrap">
              <thead className="sticky top-0 bg-card z-10 shadow-sm ring-1 ring-border/50">
                <tr className="text-left text-muted-foreground border-b border-border uppercase tracking-widest font-black">
                  <th className="p-2">Symbol</th>
                  <th className="p-2">ORC</th>
                  <th className="p-2">Side</th>
                  <th className="p-2">ADTV</th>
                  <th className="p-2">LTP</th>
                  <th className="p-2">Ent</th>
                  <th className="p-2">Dist%</th>
                  <th className="p-2">RSI</th>
                  <th className="p-2 text-right">Potential</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-foreground/90">
                {proximityWatchlist.map(stock => (
                  <tr key={stock.symbol} className="hover:bg-foreground/[0.02] group">
                    <td className="p-2 font-black border-r border-border/30">
                      <div className="flex items-center gap-1.5">
                        <a
                          href={getGoogleFinanceLink(stock.symbol)}
                          target="_blank"
                          rel="noreferrer"
                          className="text-blue-500 hover:text-blue-400 flex items-center gap-1 transition-colors"
                        >
                          <ExternalLink size={10} className="text-primary opacity-50" />
                          {stock.symbol}
                        </a>
                      </div>
                    </td>
                    <td className="p-2 border-r border-border/30">{getOracleBadge(stock.oracle_status)}</td>
                    <td className="p-2 border-r border-border/30">
                      <span className={cn(
                        "px-1.5 py-0.5 rounded text-[8px] font-black uppercase",
                        stock.side === 'LONG' ? "bg-green-500/10 text-green-500" : "bg-red-500/10 text-red-500"
                      )}>
                        {stock.side}
                      </span>
                    </td>
                    <td className="p-2 font-mono opacity-80 border-r border-border/30">{formatNumber(stock.avg_daily_turnover, 0)} Cr</td>
                    <td className="p-2 font-mono font-bold border-r border-border/30 text-amber-500">₹{formatNumber(stock.live_price)}</td>
                    <td className="p-2 font-mono border-r border-border/30 opacity-70">₹{formatNumber(stock.target_price)}</td>
                    <td className={cn("p-2 font-mono font-black border-r border-border/30", stock.distance < 0.3 ? "text-primary animate-pulse" : "text-foreground")}>
                      {stock.distance.toFixed(2)}%
                    </td>
                    <td className="p-2 font-mono border-r border-border/30 opacity-70">{formatNumber(stock.rsi, 1)}</td>
                    <td className="p-2 text-right">
                      <span className={cn(
                        "px-2 py-0.5 rounded-[4px] text-[8px] font-black uppercase tracking-widest",
                        stock.near_type === 'BREAKOUT' ? "bg-green-500 text-white" : "bg-red-500 text-white"
                      )}>
                        {stock.near_type}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* 2. Logged Signals (Post-Breakout Audit) */}
      {sortedLogs.length > 0 && (
        <section className="glass rounded-2xl border border-border overflow-hidden flex flex-col animate-in fade-in slide-in-from-bottom-8 duration-700">
          <div className="p-3 border-b border-border bg-foreground/[0.02] flex items-center justify-between shrink-0">
            <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-muted-foreground">
              <ZapOff className="w-4 h-4" /> Filtered Breakouts (Post-Entry Audit)
            </h2>
            <span className="text-[10px] font-bold bg-foreground/10 px-2 py-1 rounded text-muted-foreground uppercase tracking-widest">
              {sortedLogs.length} LOGGED FLAGS
            </span>
          </div>

          <div className="overflow-auto max-h-[400px]">
            <table className="w-full text-[10px] whitespace-nowrap">
              <thead className="sticky top-0 bg-card z-10 shadow-sm ring-1 ring-border/50">
                <tr className="text-left text-muted-foreground border-b border-border uppercase tracking-widest font-black">
                  <SortableHeader label="Time" sortKey="timestamp" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Symbol" sortKey="symbol" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="ORC" sortKey="oracle_status" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Side" sortKey="side" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Bid%" sortKey="bid_pct" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Ask%" sortKey="ask_pct" currentSort={logSort} onSort={handleLogSort} />
                  <th className="p-2">Reason</th>
                  <SortableHeader label="ADTV" sortKey="adtv_cr" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Ent" sortKey="entry_price" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="RSI" sortKey="rsi" currentSort={logSort} onSort={handleLogSort} />
                  <SortableHeader label="Vol" sortKey="vol_surge" currentSort={logSort} onSort={handleLogSort} />
                  <th className="p-2 text-center sticky right-0 bg-card/95 backdrop-blur shadow-l border-l border-border">Action</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border text-foreground/90">
                {sortedLogs.map(sig => {
                  const m = sig.metrics || {};
                  const reasons = Array.isArray(sig.fail_reasons) ? sig.fail_reasons : [];
                  const ts = sig.timestamp || sig.date;

                  return (
                    <tr key={sig.id} className="hover:bg-foreground/[0.02] transition-colors group">
                      <td className="p-2 font-mono text-muted-foreground border-r border-border/30">
                        {new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false })}
                      </td>
                      <td className="p-2 border-r border-border/30 relative group/symbol">
                        <div className="flex items-center gap-1.5">
                          <a
                            href={getGoogleFinanceLink(sig.symbol)}
                            target="_blank"
                            rel="noreferrer"
                            className="text-blue-500 font-black hover:text-blue-400 flex items-center gap-1 transition-colors"
                          >
                            <ExternalLink size={10} className="text-primary opacity-50" />
                            {sig.symbol}
                          </a>
                        </div>
                        <SignalMetricsTooltip metrics={sig.metrics} />
                      </td>
                      <td className="p-2 border-r border-border/30">{getOracleBadge(sig.oracle_status || m.oracle_status)}</td>
                      <td className="p-2 border-r border-border/30">
                        <span className={cn(
                          "px-1.5 py-0.5 rounded text-[8px] font-black uppercase",
                          sig.side === 'LONG' ? "bg-green-500/10 text-green-500" : "bg-red-500/10 text-red-500"
                        )}>
                          {sig.side}
                        </span>
                      </td>
                      <td className="p-2 border-r border-border/30 font-mono font-black text-green-500 text-[11px]">
                        {formatNumber(sig.bid_pct, 1)}%
                      </td>
                      <td className="p-2 border-r border-border/30 font-mono font-black text-red-500 text-[11px]">
                        {formatNumber(sig.ask_pct, 1)}%
                      </td>
                      <td className="p-2 border-r border-border/30 max-w-[200px] truncate">
                        <div className="flex flex-wrap gap-1">
                          {reasons.map((r, i) => (
                            <span key={i} className="bg-red-500/5 text-red-500/70 border border-red-500/10 px-1.5 py-0.5 rounded-[4px] text-[8px] font-bold flex items-center gap-1">
                              <ShieldAlert size={8} /> {r}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="p-2 font-mono opacity-80 border-r border-border/30">{formatNumber(m.adtv_cr, 0)} Cr</td>
                      <td className="p-2 font-mono font-bold border-r border-border/30">₹{formatNumber(sig.entry_price)}</td>
                      <td className="p-2 font-mono border-r border-border/30">{formatNumber(m.rsi, 1)}</td>
                      <td className="p-2 font-mono font-bold text-amber-500 border-r border-border/30">{formatNumber(m.vol_surge, 1)}x</td>
                      <td className="p-2 text-center sticky right-0 bg-card/90 backdrop-blur border-l border-border group-hover:bg-accent/5">
                        <button
                          onClick={() => promoteMutation.mutate(sig.id)}
                          disabled={promoteMutation.isPending}
                          className="bg-primary/10 hover:bg-primary text-primary hover:text-primary-foreground px-2 py-1 rounded text-[8px] font-black uppercase tracking-wider flex items-center justify-center gap-1 mx-auto w-full transition-all disabled:opacity-50"
                        >
                          <MoveRight size={8} /> MOVE_TO_SIGNAL
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {proximityWatchlist.length === 0 && sortedLogs.length === 0 && (
        <section className="glass rounded-2xl border border-border p-12 text-center animate-in fade-in duration-500">
          <ZapOff className="w-8 h-8 text-muted-foreground/20 mx-auto mb-3" />
          <h3 className="text-sm font-black uppercase tracking-widest text-muted-foreground/40">Zero Near Misses Yet</h3>
          <p className="text-[10px] text-muted-foreground/30 font-bold mt-1 uppercase tracking-tighter italic">Tracking symbols getting ready to strike or those filtered out...</p>
        </section>
      )}
    </div>
  );
};

export default NearMissTable;
