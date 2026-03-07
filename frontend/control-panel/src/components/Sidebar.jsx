import { NavLink } from 'react-router-dom';
import { LayoutDashboard, Target, Settings, Activity, ShieldCheck, Sun, Moon } from 'lucide-react';
import { cn } from '../lib/utils';
import { useTheme } from '../lib/ThemeContext';

const Sidebar = () => {
  const { theme, toggleTheme } = useTheme();
  const menuItems = [
    { icon: LayoutDashboard, label: 'Dashboard', path: '/' },
    { icon: Target, label: 'ORB Strategy', path: '/orb-strategy' },
    { icon: Activity, label: 'Dhan Orders', path: '/dhan-orders' },
    {
      icon: Settings,
      label: 'Settings',
      path: '/settings',
      children: [
        { label: 'ORB Signal Generator', path: '/settings/signal-generator' },
        { label: 'Intraday Feeder', path: '/settings/intraday-feeder' },
        { label: 'Oracle Sync', path: '/settings/oracle-sync' },
        { label: 'ORB Calculator', path: '/settings/orb-calculator' },
        { label: 'Portfolio Manager', path: '/settings/portfolio-manager' },
      ]
    },
    { icon: Activity, label: 'Activity', path: '/activity' },
  ];

  return (
    <aside className="w-64 bg-card border-r h-screen sticky top-0 flex flex-col shrink-0">
      <div className="p-5 flex items-center gap-3 border-b mb-4">
        <ShieldCheck className="w-6 h-6 text-blue-600" />
        <span className="font-bold text-lg tracking-tight">Mimic Lab</span>
      </div>

      <nav className="flex-1 px-3 space-y-1 overflow-y-auto">
        {menuItems.map((item) => (
          <div key={item.label}>
            <NavLink
              to={item.path}
              end={item.children ? false : true}
              className={({ isActive }) => cn(
                "flex items-center gap-3 px-3 py-2 rounded-lg transition-all duration-200 group mb-0.5",
                isActive && !item.children
                  ? "bg-primary/10 text-primary shadow-sm"
                  : "text-muted-foreground hover:bg-accent hover:text-foreground"
              )}
            >
              <item.icon className="w-4 h-4 group-hover:scale-110 transition-transform" />
              <span className="text-sm font-medium">{item.label}</span>
            </NavLink>

            {/* SUBMENU */}
            {item.children && (
              <div className="ml-9 space-y-0.5 mt-0.5 border-l border-border/40 pl-2">
                {item.children.map(sub => (
                  <NavLink
                    key={sub.path}
                    to={sub.path}
                    className={({ isActive }) => cn(
                      "block px-3 py-1.5 rounded-md text-[11px] font-medium transition-colors hover:bg-accent hover:text-foreground",
                      isActive ? "bg-accent text-primary font-bold" : "text-muted-foreground/70"
                    )}
                  >
                    {sub.label}
                  </NavLink>
                ))}
              </div>
            )}
          </div>
        ))}
      </nav>

      <div className="p-4 border-t space-y-4">
        <button
          onClick={toggleTheme}
          className="w-full flex items-center justify-between px-3 py-2 rounded-lg bg-accent/50 hover:bg-accent transition-colors text-sm"
        >
          <span className="text-muted-foreground">Mode</span>
          {theme === 'light' ? <Moon className="w-4 h-4" /> : <Sun className="w-4 h-4" />}
        </button>
        <div className="text-[10px] text-muted-foreground text-center opacity-50 uppercase tracking-widest">
          v4.1.0 • Stable
        </div>
      </div>
    </aside>
  );
};

export default Sidebar;
