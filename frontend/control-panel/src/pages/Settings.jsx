import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getSystemJobs, getJobConfig, updateJobConfig } from '../services/api';
import { Save, RefreshCw, Settings as SettingsIcon, Sliders, ChevronRight } from 'lucide-react';
import { useState } from 'react';
import { cn } from '../lib/utils';

const Settings = () => {
  const queryClient = useQueryClient();
  const { data: jobs, isLoading: jobsLoading } = useQuery({ queryKey: ['jobs'], queryFn: getSystemJobs });
  const [selectedJobId, setSelectedJobId] = useState('');

  const { data: config, isLoading: configLoading } = useQuery({
    queryKey: ['job-config', selectedJobId],
    queryFn: () => getJobConfig(selectedJobId),
    enabled: !!selectedJobId
  });

  const mutation = useMutation({
    mutationFn: (newConfig) => updateJobConfig(selectedJobId, newConfig),
    onSuccess: () => {
      queryClient.invalidateQueries(['job-config', selectedJobId]);
    }
  });

  const handleSave = (e) => {
    e.preventDefault();
    const formData = new FormData(e.target);
    const updatedConfig = {};

    // Process form data with type conversion
    Object.keys(config).forEach(key => {
      const val = formData.get(key);
      if (typeof config[key] === 'boolean') {
        updatedConfig[key] = formData.has(key);
      } else if (typeof config[key] === 'number') {
        updatedConfig[key] = parseFloat(val);
      } else if (Array.isArray(config[key])) {
        updatedConfig[key] = formData.getAll(key);
      } else {
        updatedConfig[key] = val;
      }
    });

    mutation.mutate(updatedConfig);
  };

  const renderInputField = (key, value) => {
    const label = key.replace(/_/g, ' ').toUpperCase();

    // BOOLEAN -> TOGGLE
    if (typeof value === 'boolean') {
      return (
        <div key={key} className="flex items-center justify-between p-4 bg-accent/30 rounded-xl border border-border/50 transition-all hover:bg-accent/50">
          <div>
            <p className="text-xs font-bold tracking-widest text-muted-foreground mb-1">{label}</p>
            <p className="text-[10px] text-muted-foreground/60 italic">Enable or disable this feature</p>
          </div>
          <label className="relative inline-flex items-center cursor-pointer">
            <input type="checkbox" name={key} defaultChecked={value} className="sr-only peer" />
            <div className="w-11 h-6 bg-slate-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-primary shadow-inner"></div>
          </label>
        </div>
      );
    }

    // LIST -> MULTI-SELECT (Simple checkbox list for parity)
    if (Array.isArray(value)) {
      const options = key.includes('sniper') ? ['UP_SNIPER', 'DOWN_SNIPER'] : [];
      return (
        <div key={key} className="p-4 bg-accent/30 rounded-xl border border-border/50">
          <p className="text-xs font-bold tracking-widest text-muted-foreground mb-3">{label}</p>
          <div className="flex flex-wrap gap-2">
            {options.map(opt => (
              <label key={opt} className="flex items-center gap-2 bg-background/50 px-3 py-1.5 rounded-lg border border-border/50 cursor-pointer hover:bg-background transition-colors">
                <input type="checkbox" name={key} value={opt} defaultChecked={value.includes(opt)} className="rounded border-border text-primary focus:ring-primary shadow-sm" />
                <span className="text-xs font-medium">{opt.replace('_', ' ')}</span>
              </label>
            ))}
          </div>
        </div>
      );
    }

    // NUMBER -> SLIDER (if looks like a pct or small range)
    const isSlider = key.endsWith('_pct') || key.includes('vol_') || key.includes('rsi') || key.includes('_min') || key.includes('_max');
    if (typeof value === 'number' && isSlider) {
      let min = 0, max = 100, step = 1;
      if (key.includes('pct')) { max = 10; step = 0.1; }
      if (key.includes('vol')) { max = 25; step = 0.05; }
      if (key.includes('macd')) { min = -1; max = 1; step = 0.01; }
      if (key.includes('trend')) { min = -0.5; max = 0.5; step = 0.01; }

      return (
        <div key={key} className="p-4 bg-accent/30 rounded-xl border border-border/50 space-y-3">
          <div className="flex justify-between items-center">
            <p className="text-xs font-bold tracking-widest text-muted-foreground">{label}</p>
            <span className="text-xs font-mono bg-primary/10 text-primary px-2 py-0.5 rounded border border-primary/20">{value}</span>
          </div>
          <input
            type="range"
            name={key}
            defaultValue={value}
            min={min}
            max={max}
            step={step}
            className="w-full h-1.5 bg-slate-700 rounded-lg appearance-none cursor-pointer accent-primary"
            onChange={(e) => {
              e.target.previousSibling.lastChild.innerText = e.target.value;
            }}
          />
        </div>
      );
    }

    // DEFAULT -> INPUT
    return (
      <div key={key} className="p-4 bg-accent/30 rounded-xl border border-border/50 space-y-2">
        <label className="text-xs font-bold tracking-widest text-muted-foreground">{label}</label>
        <input
          name={key}
          defaultValue={value}
          className="w-full px-4 py-2 text-sm rounded-lg border border-border/50 bg-background/50 focus:bg-background focus:ring-2 focus:ring-primary/20 transition-all outline-none shadow-inner"
        />
      </div>
    );
  };

  return (
    <div className="p-6 space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <header className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2.5 bg-primary/10 rounded-xl shadow-sm border border-primary/20">
            <SettingsIcon className="w-5 h-5 text-primary" />
          </div>
          <div>
            <h1 className="text-xl font-bold tracking-tight">System Settings</h1>
            <p className="text-[11px] text-muted-foreground uppercase tracking-widest opacity-60">Architectural Control Center</p>
          </div>
        </div>
      </header>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        {/* SIDEBAR SELECTOR */}
        <aside className="lg:col-span-3 space-y-2">
          {jobsLoading ? <div className="p-4 bg-accent/20 rounded-xl border animate-pulse">Loading...</div> : jobs?.map(job => (
            <button
              key={job.job_id}
              onClick={() => setSelectedJobId(job.job_id)}
              className={cn(
                "w-full flex items-center justify-between px-4 py-3 rounded-xl border transition-all duration-300 group",
                selectedJobId === job.job_id
                  ? 'bg-primary text-white border-primary shadow-blue-500/20 shadow-lg scale-105'
                  : 'bg-card hover:bg-accent border-border/50'
              )}
            >
              <span className="text-xs font-bold tracking-wider uppercase">{job.job_id.replace(/_/g, ' ')}</span>
              <ChevronRight className={cn("w-4 h-4 transition-transform", selectedJobId === job.job_id ? "translate-x-1" : "opacity-0 group-hover:opacity-40")} />
            </button>
          ))}
        </aside>

        {/* CONFIG FORM */}
        <main className="lg:col-span-9 bg-card rounded-2xl border border-border/50 p-6 shadow-xl relative overflow-hidden glass">
          <div className="absolute top-0 right-0 w-64 h-64 bg-primary/5 rounded-full -translate-y-1/2 translate-x-1/2 blur-3xl pointer-events-none" />

          {!selectedJobId ? (
            <div className="h-[400px] flex flex-col items-center justify-center text-muted-foreground opacity-40">
              <Sliders className="w-16 h-16 mb-4 stroke-[1px]" />
              <p className="text-sm tracking-widest uppercase">Select a module to adjust parameters</p>
            </div>
          ) : configLoading ? (
            <div className="flex items-center justify-center h-[400px] gap-3">
              <RefreshCw className="w-5 h-5 animate-spin text-primary" />
              <span className="text-sm font-medium tracking-wide">Syncing Configuration...</span>
            </div>
          ) : (
            <form onSubmit={handleSave} className="space-y-8">
              <div className="flex items-center justify-between border-b pb-6 border-border/50">
                <div>
                  <h3 className="text-lg font-bold capitalize tracking-tight">{selectedJobId.replace(/_/g, ' ')} Parameters</h3>
                  <p className="text-[11px] text-muted-foreground/60 italic">Modify hardware-level strategy constraints</p>
                </div>
                <button
                  type="submit"
                  disabled={mutation.isPending}
                  className="flex items-center gap-2 bg-primary text-white px-5 py-2.5 rounded-xl hover:bg-primary/90 disabled:opacity-50 transition-all font-bold text-xs uppercase tracking-widest shadow-lg shadow-primary/20 active:scale-95"
                >
                  <Save className="w-4 h-4" />
                  {mutation.isPending ? 'Syncing...' : 'Apply Changes'}
                </button>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(config || {}).map(([key, value]) => renderInputField(key, value))}
              </div>

              {Object.keys(config || {}).length === 0 && (
                <div className="flex flex-col items-center justify-center p-12 bg-accent/20 rounded-2xl border border-dashed border-border text-muted-foreground">
                  <p className="text-sm font-medium tracking-tight mt-2">Zero Config: Module has no tunable parameters.</p>
                </div>
              )}

              {mutation.isSuccess && (
                <div className="p-3 bg-green-500/10 border border-green-500/30 rounded-xl text-green-500 text-xs font-bold text-center animate-in zoom-in duration-300">
                  ⚡ SYSTEM UPDATED: Changes successfully pushed to production.
                </div>
              )}
            </form>
          )}
        </main>
      </div>
    </div>
  );
};

export default Settings;
