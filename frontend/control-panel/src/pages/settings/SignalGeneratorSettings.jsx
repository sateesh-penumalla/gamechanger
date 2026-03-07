import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { getJobConfig, updateJobConfig } from '../../services/api'; // Fix path import
import { Save, RefreshCw, Zap, TrendingUp, TrendingDown, Target, Filter, Clock } from 'lucide-react';
import { cn } from '../../lib/utils'; // Fix path import

const SectionHeader = ({ icon: Icon, title, desc }) => (
  <div className="flex items-center gap-2 mb-4 pb-2 border-b border-border/50">
    <Icon size={14} className="text-primary" />
    <div>
      <h3 className="text-xs font-black uppercase tracking-widest">{title}</h3>
      <p className="text-[9px] text-muted-foreground font-mono">{desc}</p>
    </div>
  </div>
);

const RangeInput = ({ label, nameMin, nameMax, valMin, valMax, step = 1, min = 0, max = 100 }) => (
  <div className="space-y-1.5 p-3 bg-accent/20 rounded-lg border border-border/40">
    <div className="flex justify-between">
      <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">{label}</label>
      <span className="text-[9px] font-mono text-primary bg-primary/10 px-1 rounded">{valMin} - {valMax}</span>
    </div>
    <div className="flex gap-2 items-center">
      <input type="number" name={nameMin} defaultValue={valMin} step={step} className="w-12 h-5 text-[10px] bg-background border border-border rounded px-1 text-center" />
      <span className="text-[9px] text-muted-foreground">TO</span>
      <input type="number" name={nameMax} defaultValue={valMax} step={step} className="w-12 h-5 text-[10px] bg-background border border-border rounded px-1 text-center" />
    </div>
  </div>
);

const NumberInput = ({ label, name, value, step = 0.01 }) => (
  <div className="space-y-1.5 p-3 bg-accent/20 rounded-lg border border-border/40">
    <div className="flex justify-between">
      <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">{label}</label>
      <span className="text-[9px] font-mono text-primary bg-primary/10 px-1 rounded">{value}</span>
    </div>
    <input type="number" name={name} defaultValue={value} step={step} className="w-full h-5 text-[10px] bg-background border border-border rounded px-1" />
  </div>
);

const MultiSelect = ({ label, name, value = [], options = [] }) => (
  <div className="space-y-1.5 p-3 bg-accent/20 rounded-lg border border-border/40 col-span-2">
    <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground block mb-1">{label}</label>
    <div className="flex flex-wrap gap-2">
      {options.map(opt => (
        <label key={opt} className="flex items-center gap-1.5 bg-background/50 px-2 py-1 rounded border border-border/50 cursor-pointer hover:bg-background transition-colors">
          <input type="checkbox" name={name} value={opt} defaultChecked={value.includes(opt)} className="rounded border-border text-primary focus:ring-0 w-3 h-3" />
          <span className="text-[9px] font-bold">{opt}</span>
        </label>
      ))}
    </div>
  </div>
);

const Toggle = ({ label, name, checked }) => (
  <div className="flex items-center justify-between p-3 bg-accent/20 rounded-lg border border-border/40">
    <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground">{label}</label>
    <label className="relative inline-flex items-center cursor-pointer">
      <input type="checkbox" name={name} defaultChecked={checked} className="sr-only peer" />
      <div className="w-7 h-4 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-3 after:w-3 after:transition-all peer-checked:bg-primary shadow-inner"></div>
    </label>
  </div>
);

const SignalGeneratorSettings = () => {
  const queryClient = useQueryClient();
  const jobId = 'signal_generator';
  const { data: config, isLoading } = useQuery({ queryKey: ['job-config', jobId], queryFn: () => getJobConfig(jobId) });

  const mutation = useMutation({
    mutationFn: (newConfig) => updateJobConfig(jobId, newConfig),
    onSuccess: () => {
      queryClient.invalidateQueries(['job-config', jobId]);
      toast.success('Configuration saved successfully');
    },
    onError: (error) => {
      toast.error(`Failed to save configuration: ${error.message}`);
    }
  });

  const handleSave = (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const updatedConfig = { ...config }; // merge with existing

    // Manual extraction for specific fields
    updatedConfig.min_adtv = parseFloat(formData.get('min_adtv')) || 0;
    updatedConfig.enforce_bias = formData.get('enforce_bias') === 'on';

    // Weekly RSI & SMA Alignment
    updatedConfig.weekly_rsi_l = [parseInt(formData.get('weekly_rsi_l_min')) || 60, 100];
    updatedConfig.weekly_rsi_s = [0, parseInt(formData.get('weekly_rsi_s_max')) || 40];
    updatedConfig.sma_align_l = formData.get('sma_align_l') === 'on';
    updatedConfig.sma_align_s = formData.get('sma_align_s') === 'on';

    // Ranges (Fix: Parse as float/int with guards)
    updatedConfig.range_pct_min = parseFloat(formData.get('range_pct_min')) || 0;
    updatedConfig.range_pct_max = parseFloat(formData.get('range_pct_max')) || 0;

    // Long
    updatedConfig.rsi_l_min = parseInt(formData.get('rsi_l_min')) || 0;
    updatedConfig.rsi_l_max = parseInt(formData.get('rsi_l_max')) || 0;
    updatedConfig.macd_l_min = parseFloat(formData.get('macd_l_min')) || 0;
    updatedConfig.trend_l_min = parseFloat(formData.get('trend_l_min')) || 0;
    updatedConfig.long_snipers = formData.getAll('long_snipers');

    // Short
    updatedConfig.rsi_s_min = parseInt(formData.get('rsi_s_min')) || 0;
    updatedConfig.rsi_s_max = parseInt(formData.get('rsi_s_max')) || 0;
    updatedConfig.macd_s_max = parseFloat(formData.get('macd_s_max')) || 0;
    updatedConfig.trend_s_max = parseFloat(formData.get('trend_s_max')) || 0;
    updatedConfig.short_snipers = formData.getAll('short_snipers');

    mutation.mutate(updatedConfig);
  };

  if (isLoading) return <div className="flex h-full items-center justify-center p-20 animate-pulse text-[10px] font-bold tracking-widest opacity-50">SYNCING_CORE_LOGIC...</div>;

  return (
    <form onSubmit={handleSave} className="space-y-6 animate-in fade-in zoom-in-95 duration-300 p-6 max-w-7xl mx-auto">
      <div className="flex items-center justify-between border-b border-border pb-4">
        <div>
          <h1 className="text-lg font-black tracking-tight flex items-center gap-2">
            <Zap className="w-5 h-5 text-primary" /> ORB SIGNAL GENERATOR
          </h1>
          <p className="text-[10px] text-muted-foreground font-mono uppercase tracking-widest">Autonomous Detection Engine Configuration</p>
        </div>
        <button
          type="submit"
          disabled={mutation.isPending}
          className="flex items-center gap-2 bg-primary text-white px-4 py-1.5 rounded-lg hover:bg-primary/90 disabled:opacity-50 transition-all font-black text-[10px] uppercase tracking-widest shadow-lg shadow-primary/20 active:scale-95"
        >
          <Save size={12} /> {mutation.isPending ? 'SYNCING...' : 'APPLY'}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">

        {/* COL 1: SCOPE & FILTERS */}
        <section className="space-y-4 border-2 border-border/60 rounded-xl p-5 bg-card/40 shadow-sm">
          <SectionHeader icon={Filter} title="Scope & Filters" desc="Market Liquidity & Context" />
          <NumberInput label="Min ADTV (₹ Cr)" name="min_adtv" value={config.min_adtv || 0.5} step={0.1} />
          <Toggle label="Enforce ORB Direction Bias" name="enforce_bias" checked={config.enforce_bias} />
          <RangeInput label="ORB Range %" nameMin="range_pct_min" nameMax="range_pct_max" valMin={config.range_pct_min} valMax={config.range_pct_max} step={0.1} />
        </section>

        {/* COL 2: LONG STRATEGY */}
        <section className="space-y-4 border-2 border-border/60 rounded-xl p-5 bg-card/40 shadow-sm">
          <SectionHeader icon={TrendingUp} title="Long Strategy" desc="Breakout Confirmation" />
          <div className="grid grid-cols-2 gap-3">
            <MultiSelect label="Sniper Modes" name="long_snipers" value={config.long_snipers} options={['UP_SNIPER', 'DOWN_SNIPER', 'FILTERED', 'NEUTRAL']} />
            <RangeInput label="RSI Range" nameMin="rsi_l_min" nameMax="rsi_l_max" valMin={config.rsi_l_min} valMax={config.rsi_l_max} step={1} min={0} max={100} />
            <NumberInput label="Min MACD" name="macd_l_min" value={config.macd_l_min} step={0.01} />
            <NumberInput label="Min Slope" name="trend_l_min" value={config.trend_l_min} step={0.01} />
            <div className="col-span-2 space-y-3 pt-2">
              <Toggle label="Align w/ Weekly SMA" name="sma_align_l" checked={config.sma_align_l !== false} />
              <div className="space-y-1">
                <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground block">Min Weekly RSI</label>
                <input type="number" name="weekly_rsi_l_min" defaultValue={config.weekly_rsi_l?.[0] || 60} className="w-full h-5 text-[10px] bg-background border border-border rounded px-1" />
              </div>
            </div>
          </div>
        </section>

        {/* COL 3: SHORT STRATEGY */}
        <section className="space-y-4 border-2 border-border/60 rounded-xl p-5 bg-card/40 shadow-sm">
          <SectionHeader icon={TrendingDown} title="Short Strategy" desc="Breakdown Confirmation" />
          <div className="grid grid-cols-2 gap-3">
            <MultiSelect label="Sniper Modes" name="short_snipers" value={config.short_snipers} options={['DOWN_SNIPER', 'UP_SNIPER', 'FILTERED', 'NEUTRAL']} />
            <RangeInput label="RSI Range" nameMin="rsi_s_min" nameMax="rsi_s_max" valMin={config.rsi_s_min} valMax={config.rsi_s_max} step={1} min={0} max={100} />
            <NumberInput label="Max MACD" name="macd_s_max" value={config.macd_s_max} step={0.01} />
            <NumberInput label="Max Slope" name="trend_s_max" value={config.trend_s_max} step={0.01} />
            <div className="col-span-2 space-y-3 pt-2">
              <Toggle label="Align w/ Weekly SMA" name="sma_align_s" checked={config.sma_align_s !== false} />
              <div className="space-y-1">
                <label className="text-[9px] font-bold uppercase tracking-wider text-muted-foreground block">Max Weekly RSI</label>
                <input type="number" name="weekly_rsi_s_max" defaultValue={config.weekly_rsi_l?.[1] || 40} className="w-full h-5 text-[10px] bg-background border border-border rounded px-1" />
              </div>
            </div>
          </div>
        </section>
      </div>
    </form>
  );
};

export default SignalGeneratorSettings;
