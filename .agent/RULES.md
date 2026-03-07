# BharatQuant MAS: Rules of Engagement

To ensure the highest quality recommendations and protect trading capital, all agents must adhere to these non-negotiable rules.

## Rule 1: Confidence Threshold
- No recommendation shall be issued with a confidence score below **85%**.
- A score of **95%+** is required for "High Conviction" status.

## Rule 2: The "Judge's" Unbiased Veto
- If any single agent (Newsroom, Globalist, or Librarian) issues a "Strong Disagree" or "Caution" signal, the Judge **must** downgrade the recommendation or veto it entirely, regardless of technical strength.

## Rule 3: Data Integrity
- Recommendations must be based on data no older than **60 seconds** (Real-time sync).
- If the Yahoo Finance API returns stale or inconsistent data, the agent must enter a "Data Freeze" mode and stop recommending.

## Rule 4: Learning from Failure (The Librarian's Rule)
- If a specific setup (e.g., "Reliance Breakout on 15-min range") has failed more than twice in the last 5 sessions, the system is prohibited from recommending that specific setup for 48 hours.

## Rule 5: Sector Exposure
- The system shall not recommend more than 2 stocks from the same sector simultaneously to prevent over-exposure to sector-specific crashes.

## Rule 6: Stop Loss & Profit Booking
- No trade is complete without a predefined Stop Loss and at least two Profit Booking targets (T1 and T2).
