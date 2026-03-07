"""
macOS System Notification Utility
Sends native macOS notifications using osascript.
"""
import subprocess
import platform


def send_macos_notification(title, message, sound="Glass"):
    """
    Send a macOS system notification.
    
    Args:
        title (str): Notification title
        message (str): Notification message
        sound (str): Sound name (default: "Glass")
                     Options: "Basso", "Blow", "Bottle", "Frog", "Funk", "Glass", 
                              "Hero", "Morse", "Ping", "Pop", "Purr", "Sosumi", "Submarine", "Tink"
    """
    # Only works on macOS
    if platform.system() != "Darwin":
        print(f"System notifications only supported on macOS. Skipping: {title} - {message}")
        return
    
    try:
        # Escape quotes in title and message
        title = title.replace('"', '\\"')
        message = message.replace('"', '\\"')
        
        # Build AppleScript command
        script = f'display notification "{message}" with title "{title}" sound name "{sound}"'
        
        # Execute via osascript
        subprocess.run(['osascript', '-e', script], check=True, capture_output=True)
        print(f"✅ Notification sent: {title} - {message}")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to send notification: {e}")
    except Exception as e:
        print(f"❌ Error sending notification: {e}")


def notify_new_signal(symbol, side, signal_type, entry_price, message=None):
    """
    Send notification for a new trading signal.
    
    Args:
        symbol (str): Stock symbol
        side (str): LONG or SHORT
        signal_type (str): BREAKOUT, BREAKDOWN, or PURE_ALPHA
        entry_price (float): Entry price
        message (str, optional): Additional context
    """
    emoji = "🟢" if side == "LONG" else "🔴"
    title = f"{emoji} New {side} Signal"
    
    if message:
        body = f"{symbol} ({signal_type}) @ ₹{entry_price:.2f}\n{message}"
    else:
        body = f"{symbol} {signal_type} @ ₹{entry_price:.2f}"
        
    send_macos_notification(title, body, sound="Glass")


def notify_signal_update(symbol, old_status, new_status):
    """
    Send notification for a signal status update.
    
    Args:
        symbol (str): Stock symbol
        old_status (str): Previous status
        new_status (str): New status
    """
    emoji_map = {
        "TARGET_HIT": "🎯",
        "SL_HIT": "🛑",
        "EXECUTED": "✅",
        "CANCELLED": "❌"
    }
    emoji = emoji_map.get(new_status, "🔔")
    title = f"{emoji} Signal Update: {symbol}"
    message = f"{old_status} → {new_status}"
    
    # Different sounds for different outcomes
    sound = "Hero" if new_status == "TARGET_HIT" else "Basso" if new_status == "SL_HIT" else "Ping"
    send_macos_notification(title, message, sound=sound)
