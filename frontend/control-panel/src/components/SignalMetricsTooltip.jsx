import React from 'react';
import { Activity } from 'lucide-react';
import { cn, formatNumber } from '../lib/utils';

const SignalMetricsTooltip = ({ metrics }) => {
  if (!metrics) return null;

  return (
    <div className="absolute left-full ml-4 top-0 z-[100] glass p-3 border border-border rounded-xl shadow-2xl w-[220px] opacity-0 group-hover/symbol:opacity-100 transition-all duration-300 pointer-events-none scale-95 group-hover/symbol:scale-100 origin-left border-l-primary/50 border-l-2">
      <div className="absolute -left-1 top-4 w-2 h-2 bg-primary rotate-45" />
      <h4 className="text-[10px] font-black uppercase tracking-widest mb-2 border-b border-border pb-1 text-primary flex items-center gap-1.5">
        <Activity size={10} /> Signal Execution Metrics
      </h4>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 font-mono text-[9px]">
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">RSI</span>
          <span className="font-bold text-foreground">{formatNumber(metrics.rsi, 1)}</span>
        </div>
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">MACD</span>
          <span className="font-bold text-foreground">{formatNumber(metrics.macd, 2)}</span>
        </div>
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">VQS</span>
          <span className="font-bold text-foreground">{formatNumber(metrics.vqs, 2)}</span>
        </div>
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">Slope</span>
          <span className="font-bold text-foreground">{formatNumber(metrics.slope, 3)}</span>
        </div>
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">V-Surge</span>
          <span className="font-bold text-amber-500">{formatNumber(metrics.vol_surge, 1)}x</span>
        </div>
        <div className="flex justify-between border-b border-border/20 pb-0.5">
          <span className="text-muted-foreground uppercase text-[8px]">Imbal</span>
          <span className="font-bold text-green-500">{formatNumber(metrics.imbalance, 1)}</span>
        </div>

        <div className="col-span-2 pt-1 border-t border-border/40 mt-1">
          <div className="flex items-center justify-between mb-1">
            <span className="text-[8px] font-black text-muted-foreground uppercase">Weekly Context</span>
            <span className={cn(
              "text-[7px] px-1 rounded uppercase font-bold",
              metrics.oracle_status?.includes('UP') ? "bg-green-500/10 text-green-500" : "bg-red-500/10 text-red-500"
            )}>{metrics.oracle_status}</span>
          </div>
          <div className="flex gap-4">
            <div className="flex-1 flex justify-between font-bold border-r border-border/20 pr-2">
              <span className="text-muted-foreground text-[8px] uppercase">WRSI</span>
              <span className="text-foreground">{formatNumber(metrics.weekly_rsi, 1)}</span>
            </div>
            <div className="flex-1 flex justify-between font-bold">
              <span className="text-muted-foreground text-[8px] uppercase">ADTV</span>
              <span className="text-foreground">{formatNumber(metrics.adtv_cr, 0)}Cr</span>
            </div>
          </div>
        </div>
      </div>
      <div className="mt-2 pt-1.5 border-t border-border flex justify-between items-center opacity-40">
        <span className="text-[7px] uppercase font-bold tracking-tighter">Capture Sync: OK</span>
        <Activity size={8} className="animate-pulse text-primary" />
      </div>
    </div>
  );
};

export default SignalMetricsTooltip;
