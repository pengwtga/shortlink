#!/usr/bin/env python3
"""用结构化描述生成时序图 SVG。

手写 SVG 时序图要手工对齐生命线、消息 y、alt/loop 框，极易错位。
这里把「参与者 / 消息 / 自调用 / 便签 / 嵌套块」描述成数据，坐标全部由程序布局。

用法：
    python3 render_diagrams.py

直接覆盖同目录下的 4 张 seq-*.svg。要改图就改文件末尾的描述
（参与者列表 + 事件列表）再重跑，不要手工编辑生成出来的 SVG。

范围：仅 3.1 / 3.2 / 3.3 / 6.5 崩溃窗口这 4 张时序图。
2.1 分层架构、2.2 读写路径、6.5 投递语义三张是手写的 SVG，不在此脚本内。
"""
import os

FONT = ("'Helvetica Neue',Helvetica,Arial,'PingFang SC',"
        "'Hiragino Sans GB','Microsoft YaHei',sans-serif")

# 分层配色，与 architecture-layers.svg 保持一致
TIER = {
    'client': ('#eef2ff', '#818cf8', '#4338ca'),
    'ctrl':   ('#ecfdf5', '#34d399', '#047857'),
    'svc':    ('#eff6ff', '#60a5fa', '#1d4ed8'),
    'infra':  ('#f5f3ff', '#a78bfa', '#6d28d9'),
    'store':  ('#fef2f2', '#fca5a5', '#b91c1c'),
}

PITCH_MSG = 36          # 一条消息占的高度
PITCH_SELF = 46         # 自调用
PITCH_NOTE = 46         # 便签
BLOCK_TOP = 26          # 块顶部（标签条 + 留白）
BLOCK_BOTTOM = 12       # 块底部（留白 + 边框）
BLOCK_ELSE = 30         # else 分隔线 + 标签
BODY_GAP = 36           # 生命线顶部到第一条消息的距离


def esc(s):
    return s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


def tw(s, size):
    """粗略估算文本宽度：CJK 记 1.0em，其余 0.56em"""
    return sum(size * (1.0 if ord(c) > 0x2E80 else 0.56) for c in s)


def circled(n):
    return chr(0x2460 + n - 1) if 1 <= n <= 20 else '(%d)' % n


# ---------- 描述 DSL ----------
def msg(f, t, label, num=None):
    return {'type': 'msg', 'from': f, 'to': t, 'label': label, 'num': num, 'dashed': False}


def ret(f, t, label, num=None):
    return {'type': 'msg', 'from': f, 'to': t, 'label': label, 'num': num, 'dashed': True}


def selfm(actor, label, num=None):
    return {'type': 'self', 'actor': actor, 'label': label, 'num': num}


def note(over, label):
    return {'type': 'note', 'over': list(over), 'label': label}


def blk(kind, label, events, else_label=None, else_events=None):
    return {'type': 'block', 'kind': kind, 'label': label, 'events': events,
            'else_label': else_label, 'else_events': else_events}


def pids(events):
    s = set()
    for e in events:
        if e['type'] == 'msg':
            s.add(e['from'])
            s.add(e['to'])
        elif e['type'] == 'self':
            s.add(e['actor'])
        elif e['type'] == 'note':
            s |= set(e['over'])
        elif e['type'] == 'block':
            s |= pids(e['events'])
            if e.get('else_events'):
                s |= pids(e['else_events'])
    return s


class Seq:
    def __init__(self, title, participants, events):
        self.title = title
        self.parts = participants            # [(id, label, tier)]
        self.events = events
        self.byid = {p[0]: i for i, p in enumerate(participants)}
        self.boxw = max(max(120.0, tw(p[1], 11) + 28) for p in participants)
        step = self.boxw + 48
        self.xs = [30 + self.boxw / 2 + i * step for i in range(len(participants))]
        self.top = 64
        self.boxh = 44
        self.body_top = self.top + self.boxh
        # 画布宽度必须覆盖所有参与者框（事件只会用到生命线，框会超出）
        self.minleft = min(self.xs) - self.boxw / 2
        self.maxright = max(self.xs) + self.boxw / 2
        self.out = []
        self.y = 0

    def x(self, pid):
        return self.xs[self.byid[pid]]

    # ---------- 基础绘制 ----------
    def track(self, x0, x1):
        self.minleft = min(self.minleft, x0)
        self.maxright = max(self.maxright, x1)

    def text(self, x, y, s, size=11.5, fill='#334155', anchor='start', weight=None):
        extra = ' font-weight="%s"' % weight if weight else ''
        self.out.append('<text x="%.1f" y="%.1f" font-size="%s" fill="%s" '
                        'text-anchor="%s"%s>%s</text>'
                        % (x, y, size, fill, anchor, extra, esc(s)))
        w = tw(s, size)
        if anchor == 'middle':
            self.track(x - w / 2, x + w / 2)
        elif anchor == 'end':
            self.track(x - w, x)
        else:
            self.track(x, x + w)

    def numtext(self, x, y, n):
        self.out.append('<circle cx="%.1f" cy="%.1f" r="6.5" fill="#f1f5f9" stroke="#cbd5e1"/>'
                        % (x, y - 3.6))
        self.out.append('<text x="%.1f" y="%.1f" font-size="10" fill="#64748b" '
                        'text-anchor="middle">%s</text>' % (x, y, circled(n)))
        self.track(x - 7, x + 7)

    def label(self, cx, y, e):
        s = e['label']
        numw = 20.0 if e.get('num') else 0.0
        left = cx - (tw(s, 11.5) + numw) / 2
        if e.get('num'):
            self.numtext(left + 7, y, e['num'])
        self.text(left + numw, y, s)

    # ---------- 度量（必须与 render 完全一致） ----------
    def measure(self, events):
        h = 0
        for e in events:
            if e['type'] == 'msg':
                h += PITCH_MSG
            elif e['type'] == 'self':
                h += PITCH_SELF
            elif e['type'] == 'note':
                h += PITCH_NOTE
            elif e['type'] == 'block':
                h += BLOCK_TOP + self.measure(e['events']) + BLOCK_BOTTOM
                if e.get('else_events') is not None:
                    h += BLOCK_ELSE + self.measure(e['else_events'])
        return h

    # ---------- 渲染 ----------
    def render_events(self, events):
        for e in events:
            t = e['type']
            if t == 'msg':
                self.render_msg(e)
            elif t == 'self':
                self.render_self(e)
            elif t == 'note':
                self.render_note(e)
            else:
                self.render_block(e)

    def render_msg(self, e):
        x1, x2 = self.x(e['from']), self.x(e['to'])
        d = 1 if x2 > x1 else -1
        y = self.y
        self.out.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                        'stroke-width="1.4"%s marker-end="url(#%s)"/>'
                        % (x1, y, x2 - d * 9, y,
                           '#94a3b8' if e['dashed'] else '#475569',
                           ' stroke-dasharray="5,3"' if e['dashed'] else '',
                           'arrd' if e['dashed'] else 'arr'))
        self.track(min(x1, x2), max(x1, x2))
        self.label((x1 + x2) / 2, y - 8, e)
        self.y += PITCH_MSG

    def render_self(self, e):
        x, y = self.x(e['actor']), self.y
        self.out.append('<path d="M %.1f %.1f h 44 v 20 h -44" fill="none" stroke="#475569" '
                        'stroke-width="1.4" marker-end="url(#arr)"/>' % (x, y - 10))
        self.track(x, x + 44)
        self.out.append('<text x="%.1f" y="%.1f" font-size="11.5" fill="#334155" '
                        'text-anchor="end">%s</text>' % (x - 8, y + 2, esc(e['label'])))
        w = tw(e['label'], 11.5)
        self.track(x - 8 - w, x - 8)
        if e.get('num'):
            self.numtext(x - 16 - w, y + 2, e['num'])
        self.y += PITCH_SELF

    def render_note(self, e):
        xs = [self.x(p) for p in e['over']]
        left = min(xs) - self.boxw / 2 + 12
        right = max(xs) + self.boxw / 2 - 12
        need = tw(e['label'], 11) + 26
        if need > right - left:
            c = (left + right) / 2
            left, right = c - need / 2, c + need / 2
        y = self.y
        self.out.append('<rect x="%.1f" y="%.1f" width="%.1f" height="30" rx="4" '
                        'fill="#fefce8" stroke="#facc15"/>' % (left, y, right - left))
        self.text((left + right) / 2, y + 20, e['label'], 11, '#854d0e', 'middle')
        self.track(left, right)
        self.y += PITCH_NOTE

    def render_block(self, e):
        ids = pids(e['events'])
        if e.get('else_events') is not None:
            ids |= pids(e['else_events'])
        xs = [self.x(p) for p in ids]
        left = min(xs) - self.boxw / 2 - 16
        right = max(xs) + self.boxw / 2 + 16
        top = self.y
        self.y += BLOCK_TOP

        self.render_events(e['events'])

        if e.get('else_events') is not None:
            self.y += 4
            self.out.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#cbd5e1" '
                            'stroke-width="1" stroke-dasharray="4,3"/>'
                            % (left, self.y, right, self.y))
            self.y += 20
            self.text(left + 10, self.y, e['else_label'], 10.5, '#475569', weight='600')
            self.y += 6
            self.render_events(e['else_events'])

        # 底部：边框 → 12px 总留白
        bottom = self.y + 6
        self.out.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="6" '
                        'fill="none" stroke="#cbd5e1"/>'
                        % (left, top, right - left, bottom - top))
        tag = '%s %s' % (e['kind'], e['label'])
        self.out.append('<rect x="%.1f" y="%.1f" width="%.1f" height="18" rx="4" '
                        'fill="#f1f5f9" stroke="#cbd5e1"/>'
                        % (left + 8, top - 9, tw(tag, 10.5) + 14))
        self.text(left + 15, top + 4, tag, 10.5, '#475569')
        self.track(left, right)
        self.y = bottom + 6

    def svg(self):
        self.y = self.body_top + BODY_GAP
        self.render_events(self.events)
        bottom = self.y - 6

        dx = max(0.0, 14 - self.minleft)
        width = int(self.maxright + dx + 18)
        height = int(bottom + 20)

        head = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" '
                'height="%d" role="img" font-family="%s">' % (width, height, width, height, FONT),
                '<title>%s</title>' % esc(self.title),
                '<defs>'
                '<marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                'markerHeight="7" orient="auto">'
                '<path d="M 0 0 L 10 5 L 0 10 z" fill="#475569"/></marker>'
                '<marker id="arrd" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" '
                'markerHeight="7" orient="auto">'
                '<path d="M 0 0 L 10 5 L 0 10 z" fill="#94a3b8"/></marker>'
                '</defs>',
                '<rect x="0" y="0" width="%d" height="%d" fill="#ffffff"/>' % (width, height),
                '<text x="%.1f" y="40" font-size="16" font-weight="700" fill="#0f172a">%s</text>'
                % (dx, esc(self.title)),
                '<g transform="translate(%.1f,0)">' % dx]

        for i in range(len(self.parts)):
            x = self.xs[i]
            head.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#cbd5e1" '
                        'stroke-width="1" stroke-dasharray="5,4"/>'
                        % (x, self.body_top, x, bottom))
        for i, (pid, label, tier) in enumerate(self.parts):
            fill, stroke, tcol = TIER[tier]
            head.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%d" rx="7" fill="%s" '
                        'stroke="%s"/>'
                        % (self.xs[i] - self.boxw / 2, self.top, self.boxw, self.boxh, fill, stroke))
            head.append('<text x="%.1f" y="%.1f" font-size="11" font-weight="600" fill="%s" '
                        'text-anchor="middle">%s</text>'
                        % (self.xs[i], self.top + 26, tcol, esc(label)))
        return '\n'.join(head + self.out + ['</g>', '</svg>']) + '\n'


# 输出到脚本所在目录，与手写的 SVG 放在一起
OUT = os.path.dirname(os.path.abspath(__file__))


def write(name, svg):
    path = os.path.join(OUT, name)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(svg)
    print('%-30s %6d bytes' % (name, len(svg.encode('utf-8'))))


# ======================= 3.1 创建短链 =======================
create = Seq(
    '3.1 创建短链',
    [('C', 'Client', 'client'), ('LC', 'LinkController', 'ctrl'),
     ('RL', 'RateLimitService', 'svc'), ('R', 'Redis', 'store'),
     ('LS', 'LinkService', 'svc'), ('SF', 'SnowflakeIdGenerator', 'infra'),
     ('M', 'MySQL', 'store')],
    [
        msg('C', 'LC', 'POST /api/v1/links {longUrl, expireAt?}', 1),
        msg('LC', 'RL', 'check("create", ip, 5, 60)', 2),
        msg('RL', 'R', 'EVAL INCR sl:rl:create:{ip}', 3),
        ret('R', 'RL', '当前计数', 4),
        blk('alt', '超过 5 次/分', [ret('RL', 'C', '429 Too Many Requests', 5)]),
        msg('LC', 'LS', 'create(request)', 6),
        selfm('LS', '校验 http(s):// 前缀、expireAt 不早于当前', 7),
        msg('LS', 'SF', 'nextId()', 8),
        ret('SF', 'LS', '雪花 ID', 9),
        selfm('LS', 'Base62.encode(id) → code', 10),
        msg('LS', 'M', 'INSERT t_link', 11),
        msg('LS', 'R', 'SET sl:link:{code} = "{expireMillis}|{longUrl}" EX 24h', 12),
        ret('LS', 'C', '{code, shortUrl, longUrl, expireAt}', 13),
    ])

# ======================= 3.2 短链跳转 =======================
redirect = Seq(
    '3.2 短链跳转（读路径）',
    [('C', 'Client', 'client'), ('RC', 'RedirectController', 'ctrl'),
     ('RL', 'RateLimitService', 'svc'), ('LS', 'LinkService', 'svc'),
     ('R', 'Redis', 'store'), ('M', 'MySQL', 'store'),
     ('SS', 'StatsService', 'svc'), ('K', 'Kafka', 'infra')],
    [
        msg('C', 'RC', 'GET /{code}', 1),
        msg('RC', 'RL', 'check("redirect", ip, 120, 60)', 2),
        msg('RL', 'R', 'EVAL INCR sl:rl:redirect:{ip}', 3),
        blk('alt', '超过 120 次/分', [ret('RL', 'C', '429', 4)]),
        msg('RC', 'LS', 'resolve(code)', 5),
        msg('LS', 'R', 'GET sl:link:{code}', 6),
        blk('alt', '缓存未命中',
            [
                msg('LS', 'M', 'SELECT t_link WHERE code = ?', 7),
                blk('alt', '不存在或 status=0',
                    [
                        msg('LS', 'R', 'SET sl:link:{code} = "__NULL__" EX 5min', 8),
                        ret('LS', 'RC', '404', 9),
                    ],
                    else_label='存在',
                    else_events=[msg('LS', 'R', 'SET sl:link:{code} EX 24h（回填）', 10)]),
            ],
            else_label='命中空值占位',
            else_events=[ret('LS', 'RC', '404（不打 DB）', 11)]),
        selfm('LS', '解析 expireMillis，已过期则 410', 12),
        ret('LS', 'RC', 'longUrl', 13),
        msg('RC', 'SS', 'recordClick(code, ip, ua, referer)', 14),
        msg('SS', 'R', 'INCR sl:cnt:{code}:{date}  EX 48h', 15),
        msg('SS', 'R', 'PFADD sl:uv:{code} + EXPIRE 180d（滑动）', 16),
        msg('SS', 'R', 'PFADD sl:uv:{code}:{date} + EXPIRE 48h', 17),
        msg('SS', 'K', 'send(topic, key=code, json)  异步、不阻塞', 18),
        ret('RC', 'C', '302 Location: longUrl', 19),
    ])

# ======================= 3.3 点击统计聚合 =======================
stats = Seq(
    '3.3 点击统计聚合',
    [('K', 'Kafka', 'infra'), ('CS', 'ClickStatsConsumer', 'infra'),
     ('SS', 'StatsService', 'svc'), ('M', 'MySQL', 'store'),
     ('SC', 'StatsController', 'ctrl'), ('R', 'Redis', 'store')],
    [
        note(['K', 'CS'], '分区分配时：用 DB 位点 seek，覆盖 Kafka 自身位点'),
        msg('CS', 'M', 'SELECT next_offset FROM t_consumer_offset', 1),
        msg('CS', 'K', 'seek(partition, next_offset)', 2),
        blk('loop', '批量拉取（默认 ≤ 500 条/批）',
            [
                msg('K', 'CS', 'List<ConsumerRecord>', 3),
                selfm('CS', '内存聚合 Map<"code|date", count>', 4),
                selfm('CS', '取每个分区批内最大 offset', 5),
                note(['CS', 'M'], '以下两条在同一事务内提交'),
                msg('CS', 'SS', 'persistDailyClicks(code, date, count)', 6),
                msg('SS', 'M', 'INSERT ... ON DUPLICATE KEY UPDATE clicks = clicks + VALUES(clicks)', 7),
                msg('CS', 'M', 'UPSERT t_consumer_offset(next_offset = maxOffset + 1)', 8),
            ]),
        note(['K', 'CS'], '事务提交后容器再提交 Kafka 位点（仅用于监控）'),
        note(['SC'], '查询时'),
        msg('SC', 'SS', 'getStats(code)', 9),
        msg('SS', 'M', 'SELECT 近 7 天日统计', 10),
        msg('SS', 'M', 'SELECT SUM(clicks) 历史总点击（真相源）', 11),
        msg('SS', 'R', 'GET sl:cnt:{code}:{today}, PFCOUNT sl:uv:{code}', 12),
        selfm('SS', 'total = mysqlTotal + max(0, redisToday - mysqlToday)', 13),
        ret('SS', 'SC', '{totalClicks, todayClicks, uv, daily[7]}', 14),
    ])

# ======================= 6.5 崩溃窗口 =======================
crash = Seq(
    '6.5 位点提交与写库之间的窗口',
    [('C', 'Consumer', 'infra'), ('M', 'MySQL', 'store'), ('K', 'Kafka', 'infra')],
    [
        msg('C', 'M', 'UPSERT clicks + N'),
        note(['C', 'K'], '← 此处崩溃 / 重平衡'),
        msg('C', 'K', 'commit offset'),
    ])

write('seq-create.svg', create.svg())
write('seq-redirect.svg', redirect.svg())
write('seq-stats.svg', stats.svg())
write('seq-crash-window.svg', crash.svg())
