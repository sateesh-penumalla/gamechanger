import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getDailyFocus, getSignals, executeSignal, getSystemJobs, demoteSignal, updateJobConfig, getBridgeStatus } from '../services/api';
import { toast } from 'sonner';
import { Zap, ExternalLink, Play, Clock, ArrowUpDown, ChevronDown, ChevronUp, List, Target, Activity, CheckCircle2, XCircle, RotateCcw, LineChart, Volume2, VolumeX, ShieldCheck, ShieldAlert } from 'lucide-react';
import { useState, useMemo, useEffect, useRef } from 'react';
import { cn, formatNumber } from '../lib/utils';
import NearMissTable from '../components/NearMissTable';
import DailyAnalysisTable from '../components/DailyAnalysisTable';
import SignalMetricsTooltip from '../components/SignalMetricsTooltip';
import { useAudioAlerts } from '../hooks/useAudioAlerts';

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

const SignalTable = ({ title, signals, onExecute, onDemote, isLoading, isPending }) => {
  const [sort, setSort] = useState({ key: 'entry_ts', direction: 'desc' });
  const getGoogleFinanceLink = (symbol) => `https://www.google.com/finance/quote/${symbol}:NSE`;

  // Helper to extract value from signal object, handling nested metrics
  const getValue = (sig, key) => {
    // 1. Direct property
    if (sig[key] !== undefined) return sig[key];

    // 2. Metrics / Entry Metrics
    const metrics = sig.metrics || sig.entry_metrics || {};
    if (metrics[key] !== undefined) return metrics[key];

    return undefined;
  };

  const sortedSignals = useMemo(() => {
    if (!signals) return [];
    return [...signals].sort((a, b) => {
      let valA = getValue(a, sort.key);
      let valB = getValue(b, sort.key);

      // Handle missing/undefined
      if (valA === undefined) valA = -Infinity;
      if (valB === undefined) valB = -Infinity;

      // String comparison
      if (typeof valA === 'string') valA = valA.toLowerCase();
      if (typeof valB === 'string') valB = valB.toLowerCase();

      if (valA < valB) return sort.direction === 'asc' ? -1 : 1;
      if (valA > valB) return sort.direction === 'asc' ? 1 : -1;
      return 0;
    });
  }, [signals, sort]);

  const handleSort = (key) => {
    setSort(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
    }));
  };


  const getStatusBadge = (status) => {
    switch (status) {
      case 'TARGET_HIT': return <span className="bg-green-500 text-white px-1.5 py-0.5 rounded text-[9px] font-black tracking-tighter flex items-center gap-1"><CheckCircle2 size={8} /> TGT HIT</span>;
      case 'SL_HIT': return <span className="bg-red-500 text-white px-1.5 py-0.5 rounded text-[9px] font-black tracking-tighter flex items-center gap-1"><XCircle size={8} /> SL HIT</span>;
      case 'EXECUTED': return <span className="bg-blue-500 text-white px-1.5 py-0.5 rounded text-[9px] font-black tracking-tighter uppercase leading-none pb-1">SENT</span>;
      case 'PENDING': return <span className="bg-amber-500/10 text-amber-500 border border-amber-500/20 px-1.5 py-0.5 rounded text-[9px] font-black">PROGRESS</span>;
      case 'TRAILING_SL': return <span className="bg-purple-500 text-white px-1.5 py-0.5 rounded text-[9px] font-black tracking-tighter flex items-center gap-1"><ShieldCheck size={8} /> TRAILING</span>;
      case 'EXITED': return <span className="bg-gray-500 text-white px-1.5 py-0.5 rounded text-[9px] font-black tracking-tighter flex items-center gap-1"><XCircle size={8} /> EXITED</span>;
      default: return <span className="bg-muted text-muted-foreground px-1.5 py-0.5 rounded text-[9px] font-bold opacity-50 uppercase tracking-tighter">{status?.replace('_', ' ')}</span>;
    }
  };

  const getOracleBadge = (status) => {
    switch (status) {
      case 'UP_SNIPER': return <span className="bg-green-500 text-white px-1.5 py-0.5 rounded text-[9px] font-extrabold tracking-tighter">UP</span>;
      case 'DOWN_SNIPER': return <span className="bg-red-500 text-white px-1.5 py-0.5 rounded text-[9px] font-extrabold tracking-tighter">DW</span>;
      case 'FILTERED': return <span className="bg-yellow-500/10 text-yellow-600 border border-yellow-500/20 px-1.5 py-0.5 rounded text-[9px] font-bold">FILT</span>;
      default: return <span className="bg-muted text-muted-foreground px-1.5 py-0.5 rounded text-[9px] font-bold opacity-50">{status || 'N/A'}</span>;
    }
  };

  return (
    <section className="glass rounded-2xl border border-border overflow-hidden flex flex-col h-[500px]">
      <div className="p-3 border-b border-border bg-foreground/[0.02] flex items-center justify-between shrink-0">
        <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2">
          <Zap className={cn("w-4 h-4", title.includes('Long') ? "text-green-500" : "text-red-500")} /> {title}
        </h2>
        <span className="text-[10px] font-bold bg-foreground/10 px-2 py-1 rounded text-muted-foreground uppercase tracking-widest">
          {signals?.length || 0} SIGNALS
        </span>
      </div>

      <div className="overflow-auto flex-1">
        <table className="w-full text-[10px] whitespace-nowrap">
          <thead className="sticky top-0 bg-card z-10 shadow-sm ring-1 ring-border/50">
            <tr className="text-left text-muted-foreground border-b border-border uppercase tracking-widest font-black">
              <SortableHeader label="Time" sortKey="timestamp" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Symbol" sortKey="symbol" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Type" sortKey="signal_type" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Bid%" sortKey="bid_pct" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Ask%" sortKey="ask_pct" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="ORC" sortKey="oracle_status" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Dir" sortKey="direction" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="ADTV" sortKey="adtv_cr" currentSort={sort} onSort={handleSort} />

              <SortableHeader label="Ent" sortKey="entry_price" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="LTP" sortKey="live_price" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Tgt" sortKey="tp" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="SL" sortKey="sl" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Status" sortKey="status" currentSort={sort} onSort={handleSort} />

              <SortableHeader label="RSI" sortKey="rsi" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="WRSI" sortKey="weekly_rsi" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="MACD" sortKey="macd" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Slope" sortKey="slope" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="Vol" sortKey="vol_surge" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="VQS" sortKey="vqs" currentSort={sort} onSort={handleSort} />

              <SortableHeader label="RNG%" sortKey="range_pct" currentSort={sort} onSort={handleSort} />
              <SortableHeader label="CLN%" sortKey="range_pct_clean" currentSort={sort} onSort={handleSort} />
              <th className="p-2 text-center sticky right-0 bg-card/95 backdrop-blur shadow-l border-l border-border">Action</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border text-foreground/90">
            {isLoading ? (
              <tr><td colSpan="21" className="p-10 text-center animate-pulse tracking-widest">SYNCING_SIGNALS...</td></tr>
            ) : sortedSignals.length === 0 ? (
              <tr><td colSpan="21" className="p-20 text-center text-muted-foreground uppercase font-bold tracking-widest italic opacity-40">Zero {title} detected</td></tr>
            ) : sortedSignals.map(sig => {
              const m = sig.metrics || sig.entry_metrics || {};
              const ts = sig.timestamp || sig.entry_time;
              const isTargetHit = sig.status === 'TARGET_HIT';
              const isSLHit = sig.status === 'SL_HIT';

              return (
                <tr key={sig.id} className={cn(
                  isTargetHit ? "bg-green-500/[0.03]" :
                    isSLHit ? "bg-red-500/[0.03]" :
                      sig.status === 'TRAILING_SL' ? "bg-purple-500/[0.03]" : ""
                )}>
                  <td className="p-2 font-mono text-muted-foreground border-r border-border/30">
                    {ts ? new Date(ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false }) : '-'}
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
                    <SignalMetricsTooltip metrics={sig.metrics || sig.entry_metrics} />
                  </td>
                  <td className="p-2 border-r border-border/30 font-black uppercase text-[9px] opacity-70 italic text-muted-foreground">
                    {sig.signal_type}
                  </td>
                  <td className="p-2 border-r border-border/30 font-mono font-black text-green-500 text-[11px]">
                    {formatNumber(sig.bid_pct, 1)}%
                  </td>
                  <td className="p-2 border-r border-border/30 font-mono font-black text-red-500 text-[11px]">
                    {formatNumber(sig.ask_pct, 1)}%
                  </td>
                  <td className="p-2 border-r border-border/30">{getOracleBadge(sig.oracle_status || m.oracle_status)}</td>
                  <td className="p-2 text-[9px] font-bold opacity-70 border-r border-border/30">
                    <span className={cn(
                      "px-1 py-0.5 rounded border uppercase text-[8px] font-black",
                      sig.orb_direction === 'BULLISH' ? "bg-green-500/10 border-green-500/20 text-green-500" :
                        sig.orb_direction === 'BEARISH' ? "bg-red-500/10 border-red-500/20 text-red-500" :
                          "bg-muted border-border text-muted-foreground"
                    )}>
                      {sig.orb_direction === 'BULLISH' ? 'BULL' : sig.orb_direction === 'BEARISH' ? 'BEAR' : 'NEUT'}
                    </span>
                  </td>
                  <td className="p-2 font-mono opacity-80 border-r border-border/30">{formatNumber(sig.adtv_cr, 0)}</td>

                  <td className="p-2 font-mono font-bold border-r border-border/30">₹{formatNumber(sig.entry_price)}</td>
                  <td className={cn(
                    "p-2 font-mono font-black border-r border-border/30",
                    sig.live_price > sig.entry_price ? "text-green-500" : sig.live_price < sig.entry_price ? "text-red-500" : "text-foreground"
                  )}>
                    ₹{formatNumber(sig.live_price)}
                  </td>
                  <td className="p-2 font-mono text-green-600 border-r border-border/30">₹{formatNumber(sig.tp)}</td>
                  <td className="p-2 font-mono text-red-600 border-r border-border/30">₹{formatNumber(sig.sl)}</td>
                  <td className="p-2 border-r border-border/30">{getStatusBadge(sig.status)}</td>

                  <td className={cn("p-2 font-mono font-bold border-r border-border/30", (m.rsi > 70 || m.rsi < 30) ? "text-primary" : "")}>{formatNumber(m.rsi, 1)}</td>
                  <td className={cn(
                    "p-2 font-mono font-black border-r border-border/30",
                    m.weekly_rsi >= 60 ? "text-green-500 underline" : m.weekly_rsi <= 40 ? "text-red-500 underline" : "text-muted-foreground opacity-50"
                  )}>
                    {formatNumber(m.weekly_rsi, 1)}
                  </td>
                  <td className="p-2 font-mono border-r border-border/30">{formatNumber(m.macd, 2)}</td>
                  <td className="p-2 font-mono border-r border-border/30">{formatNumber(m.slope, 3)}</td>
                  <td className="p-2 font-mono font-bold text-amber-500 border-r border-border/30">{formatNumber(m.vol_surge, 1)}x</td>
                  <td className="p-2 font-mono opacity-70 border-r border-border/30">{formatNumber(m.vqs, 2)}</td>

                  <td className="p-2 font-mono border-r border-border/30">{formatNumber(sig.range_pct)}%</td>
                  <td className="p-2 font-mono opacity-70 border-r border-border/30">{formatNumber(sig.range_pct_clean)}%</td>

                  <td className="p-2 text-center sticky right-0 bg-card/90 backdrop-blur border-l border-border group-hover:bg-accent/5">
                    <div className="flex flex-col gap-1 max-w-[80px] mx-auto">
                      <button
                        onClick={() => onExecute(sig.id)}
                        disabled={isPending || sig.status !== 'PENDING'}
                        className={cn(
                          "px-3 py-1 rounded text-[9px] font-black uppercase tracking-wider transition-all flex items-center justify-center gap-1 w-full shadow-sm",
                          sig.status === 'PENDING'
                            ? "bg-primary/10 hover:bg-primary text-primary hover:text-primary-foreground"
                            : "bg-muted text-muted-foreground opacity-40 grayscale cursor-not-allowed"
                        )}
                      >
                        {sig.status === 'PENDING' && <Play size={8} fill="currentColor" />}
                        {sig.status === 'PENDING' ? 'EXEC' : sig.status === 'EXECUTED' ? 'SENT' : sig.status.replace('_', ' ')}
                      </button>

                      {sig.status === 'PENDING' && (
                        <button
                          onClick={() => onDemote(sig.id)}
                          className="px-2 py-0.5 rounded text-[7px] font-bold uppercase tracking-tighter transition-all hover:bg-red-500/10 text-muted-foreground hover:text-red-500 flex items-center justify-center gap-1"
                        >
                          <RotateCcw size={7} /> Demote
                        </button>
                      )}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
};

const ORBStrategy = () => {
  const queryClient = useQueryClient();
  const { isMuted, toggleMute, playSound, unlockAudio, hasInteracted } = useAudioAlerts();

  const { data: focus, isLoading: focusLoading } = useQuery({ queryKey: ['focus'], queryFn: getDailyFocus, refetchInterval: 10000 });

  const { data: signals, isLoading: sigLoading } = useQuery({ queryKey: ['signals'], queryFn: getSignals, refetchInterval: 3000 });
  const { data: jobs } = useQuery({ queryKey: ['jobs'], queryFn: getSystemJobs, refetchInterval: 5000 });
  const { data: bridgeStatus } = useQuery({ queryKey: ['bridgeStatus'], queryFn: getBridgeStatus, refetchInterval: 5000 });

  // Job Status Helpers
  const getJobStatus = (id) => {
    const job = jobs?.find(j => j.job_id === id);
    if (!job) return { status: 'UNKNOWN', last_run: '-' };
    return {
      status: job.status,
      last_run: job.last_run ? new Date(job.last_run).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }) : '-',
      next_run: job.next_run ? new Date(job.next_run).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }) : '-'
    };
  };

  const feederStatus = getJobStatus('intraday_feeder');
  const generatorStatus = getJobStatus('signal_generator');
  const orbStatus = getJobStatus('orb_calculator');

  // --- AUDIO ALERT LOGIC ---
  const [hasInitialized, setHasInitialized] = useState(false);
  const seenPairsRef = useRef(new Set()); // Store "ID-Status" pairs to detect transitions

  useEffect(() => {
    if (signals && !sigLoading) {
      const pendingSignals = signals.filter(s => s.status === 'PENDING');

      // Initialize on first load
      if (!hasInitialized) {
        pendingSignals.forEach(s => seenPairsRef.current.add(`${s.id}-${s.status}`));
        setHasInitialized(true);
        return;
      }

      let foundNew = false;
      pendingSignals.forEach(sig => {
        const pair = `${sig.id}-${sig.status}`;
        if (!seenPairsRef.current.has(pair)) {
          seenPairsRef.current.add(pair);
          foundNew = true;
        }
      });

      if (foundNew) {
        playSound('chime');
      }
    }
  }, [signals, sigLoading, hasInitialized, playSound]);

  const executeMutation = useMutation({
    mutationFn: executeSignal,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['signals'] });
      queryClient.invalidateQueries({ queryKey: ['positions'] });
    },
  });

  const demoteMutation = useMutation({
    mutationFn: demoteSignal,
    onSuccess: (data) => {
      toast.success(data.message);
      queryClient.invalidateQueries({ queryKey: ['signals'] });
      queryClient.invalidateQueries({ queryKey: ['near_miss_signals'] });
    },
    onError: (err) => {
      toast.error(err.response?.data?.detail || "Demotion failed");
    }
  });

  const updateConfigMutation = useMutation({
    mutationFn: (config) => updateJobConfig('signal_generator', config),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] });
      toast.success("Auto-Execute settings updated");
    },
    onError: (err) => {
      toast.error("Failed to update Auto-Execute settings");
    }
  });

  const genJob = jobs?.find(j => j.job_id === 'signal_generator');
  const autoLong = genJob?.config?.auto_execute_long || false;
  const autoShort = genJob?.config?.auto_execute_short || false;

  const toggleAutoExecute = (type) => {
    const currentConfig = genJob?.config || {};
    const newConfig = {
      ...currentConfig,
      [type === 'LONG' ? 'auto_execute_long' : 'auto_execute_short']: type === 'LONG' ? !autoLong : !autoShort
    };
    updateConfigMutation.mutate(newConfig);
  };

  const handleDemote = (id) => {
    const sig = signals?.find(s => s.id === id);
    if (window.confirm(`Demote ${sig?.symbol} back to Near Miss?`)) {
      demoteMutation.mutate(id);
    }
  };

  const handleExecute = (id) => {
    const sig = signals?.find(s => s.id === id);
    const symbol = sig?.symbol || 'Unknown';

    if (window.confirm(`Confirm LIVE execution for ${symbol}?`)) {
      toast.promise(executeMutation.mutateAsync(id), {
        loading: `Placing live order for ${symbol}...`,
        success: (data) => `Successfully executed ${symbol}! ID: ${data.pos_id || 'N/A'}`,
        error: (err) => `Execution failed for ${symbol}: ${err.response?.data?.detail || err.message}`,
      });
    }
  };

  const longSignals = useMemo(() => {
    const unique = new Map();
    (signals || []).filter(s => s.side === 'LONG').forEach(s => {
      if (!unique.has(s.symbol)) unique.set(s.symbol, s);
    });
    return Array.from(unique.values());
  }, [signals]);

  const shortSignals = useMemo(() => {
    const unique = new Map();
    (signals || []).filter(s => s.side === 'SHORT').forEach(s => {
      if (!unique.has(s.symbol)) unique.set(s.symbol, s);
    });
    return Array.from(unique.values());
  }, [signals]);

  return (
    <div className="p-6 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-[1920px] mx-auto">
      <header className="flex items-center justify-between pb-2 border-b border-border/50" onClick={unlockAudio}>
        <div className="flex items-center gap-4">
          <div className="p-3 bg-primary/10 rounded-2xl border border-primary/20 shadow-inner">
            <Target className="w-6 h-6 text-primary" />
          </div>
          <div>
            <h1 className="text-2xl font-black tracking-tighter uppercase italic">ORB Strategy Center</h1>
            <div className="flex gap-4 mt-1">
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest opacity-60 flex items-center gap-2">
                <span className={cn("w-1.5 h-1.5 rounded-full", feederStatus.status === 'RUNNING' ? "bg-green-500 animate-pulse" : "bg-red-500")} />
                Feeder: <span className="text-foreground/80">{feederStatus.last_run}</span>
                {feederStatus.next_run !== '-' && <span className="text-blue-400 font-bold ml-1 tracking-tight">(Next: {feederStatus.next_run})</span>}
              </p>
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest opacity-60 flex items-center gap-2 border-l border-border/50 pl-4">
                <span className={cn("w-1.5 h-1.5 rounded-full", generatorStatus.status === 'RUNNING' ? "bg-green-500 animate-pulse" : "bg-red-500")} />
                Generator: <span className="text-foreground/80">{generatorStatus.last_run}</span>
                {generatorStatus.next_run !== '-' && <span className="text-blue-400 font-bold ml-1 tracking-tight">(Next: {generatorStatus.next_run})</span>}
              </p>
              <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest opacity-60 flex items-center gap-2 border-l border-border/50 pl-4">
                <span className={cn("w-1.5 h-1.5 rounded-full", orbStatus.status === 'RUNNING' ? "bg-green-500 animate-pulse" : "bg-red-500")} />
                ORB Calc: <span className="text-foreground/80">{orbStatus.last_run}</span>
                {orbStatus.next_run !== '-' && <span className="text-yellow-500 font-black ml-1 tracking-tight">(Next: {orbStatus.next_run})</span>}
              </p>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3">
          {/* AUTO EXECUTE TOGGLES */}
          <div className="flex bg-foreground/5 p-1 rounded-xl border border-border/50 gap-1">
            <button
              onClick={() => toggleAutoExecute('LONG')}
              className={cn(
                "flex items-center gap-1.5 px-3 py-1.5 rounded-lg transition-all font-black uppercase tracking-tighter text-[9px]",
                autoLong
                  ? "bg-green-500 text-white shadow-lg shadow-green-500/20"
                  : "text-muted-foreground hover:bg-foreground/5"
              )}
            >
              {autoLong ? <ShieldCheck size={12} /> : <ShieldAlert size={12} className="opacity-50" />}
              Auto Long
            </button>
            <button
              onClick={() => toggleAutoExecute('SHORT')}
              className={cn(
                "flex items-center gap-1.5 px-3 py-1.5 rounded-lg transition-all font-black uppercase tracking-tighter text-[9px]",
                autoShort
                  ? "bg-red-500 text-white shadow-lg shadow-red-500/20"
                  : "text-muted-foreground hover:bg-foreground/5"
              )}
            >
              {autoShort ? <ShieldCheck size={12} /> : <ShieldAlert size={12} className="opacity-50" />}
              Auto Short
            </button>
          </div>

          <button
            onClick={(e) => { e.stopPropagation(); toggleMute(); }}
            className={cn(
              "flex items-center gap-2 px-4 py-2 rounded-xl border transition-all font-black uppercase tracking-widest text-[10px]",
              isMuted
                ? "bg-red-500/10 border-red-500/20 text-red-500 hover:bg-red-500/20"
                : "bg-green-500/10 border-green-500/20 text-green-500 hover:bg-green-500/20"
            )}
          >
            {isMuted ? <VolumeX size={14} /> : <Volume2 size={14} />}
            {isMuted ? "Audio Off" : "Audio On"}
            {!hasInteracted && !isMuted && <span className="ml-1 animate-pulse">(Click to Enable)</span>}
          </button>

          {/* macOS BRIDGE STATUS */}
          <div className={cn(
            "flex items-center gap-2 px-4 py-2 rounded-xl border font-black uppercase tracking-widest text-[10px]",
            bridgeStatus?.is_active
              ? "bg-blue-500/10 border-blue-500/20 text-blue-500"
              : "bg-muted border-border text-muted-foreground opacity-50"
          )}>
            <div className={cn("w-1.5 h-1.5 rounded-full", bridgeStatus?.is_active ? "bg-blue-500 animate-pulse" : "bg-muted-foreground")} />
            Mac Bridge: {bridgeStatus?.is_active ? "Live" : "Idle"}
          </div>
        </div>
      </header>

      {/* Grid changed to cols-1 for vertical stacking as requested */}
      <div className="grid grid-cols-1 gap-6">
        <SignalTable
          title="ORB Long Signals"
          signals={longSignals}
          onExecute={handleExecute}
          onDemote={handleDemote}
          isLoading={sigLoading}
          isPending={executeMutation.isPending}
        />
        <SignalTable
          title="ORB Short Signals"
          signals={shortSignals}
          onExecute={handleExecute}
          onDemote={handleDemote}
          isLoading={sigLoading}
          isPending={executeMutation.isPending}
        />
      </div>
      <DailyAnalysisTable />

      <NearMissTable playSound={playSound} />

      <section className="glass rounded-2xl border border-border overflow-hidden">
        <div className="p-3 border-b border-border flex items-center justify-between bg-foreground/[0.02]">
          <h2 className="text-sm font-black uppercase tracking-widest flex items-center gap-2 text-foreground">
            <List className="w-4 h-4 text-primary" /> ORB Focus List
          </h2>
          <div className="text-right">
            <span className="text-[10px] font-bold text-muted-foreground uppercase tracking-widest block">Market Watchlist</span>
            <span className="text-[9px] font-mono text-muted-foreground opacity-50 block">
              {focus?.length > 0 && focus[0].last_updated ?
                `Updated: ${new Date(Math.max(...focus.map(f => new Date(f.last_updated)))).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`
                : ''}
            </span>
          </div>
        </div>
        <div className="p-6">
          {focusLoading ? <div className="p-10 text-center animate-pulse font-bold tracking-widest opacity-20 italic underline">Reconnoitering focus assets...</div> : (
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5 gap-4">
              {focus?.map(stock => (
                <div key={stock.symbol} className="p-4 rounded-2xl border border-border bg-foreground/[0.01] hover:bg-foreground/[0.03] transition-all group relative overflow-hidden">
                  <div className="absolute top-0 right-0 w-16 h-16 bg-blue-500/5 rounded-full blur-2xl translate-x-1/2 -translate-y-1/2" />
                  <div className="relative z-10 flex flex-col h-full justify-between gap-4">
                    <div>
                      <h3 className="text-base font-black text-blue-500 mb-1 tracking-tight">
                        <a href={`https://www.google.com/finance/quote/${stock.symbol}:NSE`} target="_blank" rel="noreferrer" className="flex items-center gap-1 hover:text-blue-400 transition-colors">
                          <ExternalLink size={12} className="text-primary" />
                          {stock.symbol}
                        </a>
                      </h3>
                      <p className="text-[10px] text-muted-foreground font-bold flex items-center gap-1.5">
                        <Clock size={10} className="text-primary" /> Window: {stock.orb_window}m
                      </p>
                    </div>
                    <div className="pt-3 border-t border-border/50 flex items-end justify-between">
                      <div className="space-y-1">
                        <p className="text-[9px] font-black text-muted-foreground/60 uppercase">ORB High/Low • {stock.orb_direction || 'NEUTRAL'}</p>
                        <p className="text-[11px] font-mono font-bold text-foreground">{stock.orb_high} / {stock.orb_low}</p>
                      </div>
                      <div className="flex flex-col items-end gap-2">
                        <div className={cn(
                          "px-2 py-1 rounded-lg border flex flex-col items-center min-w-[50px]",
                          stock.orb_range_pct < 1.5 ? "bg-green-500/10 border-green-500/20 text-green-500" : "bg-yellow-500/10 border-yellow-500/20 text-yellow-600"
                        )}>
                          <span className="text-[8px] font-black uppercase leading-none mb-0.5">Range</span>
                          <span className="text-xs font-black">{stock.orb_range_pct?.toFixed(2)}%</span>
                        </div>

                        {/* Sentiment Bar */}
                        <div className="w-full flex flex-col items-end gap-0.5">
                          <div className="flex justify-between w-full text-[8px] font-black uppercase tracking-tighter opacity-70">
                            <span className="text-green-500">{Math.round(stock.bid_pct)}%</span>
                            <span className="text-red-500">{Math.round(stock.ask_pct)}%</span>
                          </div>
                          <div className="w-16 h-1.5 bg-muted rounded-full overflow-hidden flex border border-border/50">
                            <div className="h-full bg-green-500 transition-all duration-500" style={{ width: `${stock.bid_pct || 50}%` }} />
                            <div className="h-full bg-red-500 transition-all duration-500" style={{ width: `${stock.ask_pct || 50}%` }} />
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </section>
    </div>
  );
};

export default ORBStrategy;
