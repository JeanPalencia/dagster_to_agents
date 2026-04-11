"""Marketing channel classification based on UTM parameters."""

import re


def get_channel(source: str, medium: str, campaign_name: str) -> str:
    """Classify marketing channel based on UTM parameters."""
    src = (source or '').lower().strip()
    med = (medium or '').lower().strip()
    camp = (campaign_name or '').lower().strip()
    
    social_pattern = r'(facebook|instagram|meta|linkedin|twitter|tiktok|snapchat|pinterest|ig|lnkd\.in)'
    
    # 1. LLMs first (ChatGPT, Gemini, etc. — often arrive with empty medium)
    if re.search(r'(chatgpt|perplexity|claude|bard|gemini|anthropic|openai|llm|ai\s*chat|ai\s*assistant)', src):
        return 'Organic LLMs'
    
    # 2. Empty or null (now without LLMs trapped)
    if med == '' or (re.search(r'(l\.wl\.co|t\.co|github\.com|statics\.teams\.cdn\.office\.net)', src) and med == 'referral'):
        return 'Nulo-Vacío'
    
    # 3. Organic Search
    if (camp == '(organic)' and src not in ('adwords',) and med not in ('spot',)) \
       or med == 'organic' \
       or (src == '' and med == '' and 'comunicado' not in camp) \
       or (re.search(r'(search\.yam\.com|copilot\.microsoft\.com|search\.google\.com|msn\.com)', src) and med == 'referral') \
       or (re.search(r'(google|bing|yahoo|duckduckgo|ecosia|search|adwords)', src) 
           and med not in ('cpc', 'spot', 'paid', 'referral') 
           and not re.search(r'(cpc,|spot,|paid,|\(not set\))', med)) \
       or (re.search(r'search\.yahoo\.com', src) and med == 'referral'):
        return 'Organic Search'
    
    # 4. Mail
    if src == 'mail' or (re.search(r'(gmail|outlook|yahoo|hotmail|protonmail|icloud|zoho|mail\.ru|yandex|aol|live\.com|office365|exchange|mailchimp|sendgrid|mailgun|postmark|amazonses|sendinblue|brevo|active_campaign|active_campaing)', src) 
                         and not re.search(r'search\.', src)):
        return 'Mail'
    
    # 5. Display
    if '_display_' in camp or '_disp_' in camp:
        return 'Display'
    
    # 6. Direct
    if src == '(direct)':
        return 'Direct'
    
    # 7. Referral
    if (camp == '(referral)' or 'referral' in med) and not re.search(social_pattern, src):
        return 'Referral'
    
    # 8. Cross-network (PMAX, etc.)
    if 'cross-network' in camp or 'pmax' in camp \
       or 'syndicatedsearch.goog' in src \
       or ('google' in src and med in ('', 'cross-network')) \
       or ('nova.taboolanews.com' in src and med == 'referral'):
        return 'Cross-network'
    
    # 9. Paid Video
    if 'youtube' in camp or re.search(r'yt_', camp):
        return 'Paid Video'
    
    # 10. Paid Search
    if (med in ('cpc', 'spot', 'paid') 
        and (re.search(r'(google|bing|yahoo|duckduckgo|ecosia|search\.)', src) or 'search' in camp or 'srch' in camp)) \
       or (src == 'adwords' and med == '(not set)'):
        return 'Paid Search'
    
    # 11. Paid Social
    if med in ('cpc', 'paid', 'paid_social') and re.search(social_pattern, src):
        return 'Paid Social'
    
    # 12. Organic Social
    if med in ('social', 'rss') \
       or (re.search(social_pattern, src) and med not in ('cpc', 'paid') and not re.search(r'meta', camp)):
        return 'Organic Social'
    
    return 'Unassigned'
