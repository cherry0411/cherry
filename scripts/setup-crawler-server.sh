iptables -t raw -A PREROUTING -p udp --dport 20003:20022 -j NOTRACK
iptables -t raw -A PREROUTING -p udp --sport 20003:20022 -j NOTRACK
iptables -t raw -A OUTPUT    -p udp --dport 20003:20022 -j NOTRACK
iptables -t raw -A OUTPUT    -p udp --sport 20003:20022 -j NOTRACK

sysctl -w net.netfilter.nf_conntrack_max=2097152
sysctl -w net.netfilter.nf_conntrack_udp_timeout=10
sysctl -w net.netfilter.nf_conntrack_udp_timeout_stream=10