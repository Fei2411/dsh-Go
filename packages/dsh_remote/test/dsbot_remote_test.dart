import 'dart:async';

import 'package:dsh_remote/dsh_remote.dart';
import 'package:test/test.dart';

import 'support/fake_remote.dart';

const fastMux = MuxOptions(readyTimeout: Duration(seconds: 3), retryCaps: [Duration(milliseconds: 30)]);

void main() {
  late FakeRemoteServer server;
  late FakeRemoteHost host;
  late RemoteAccount account;

  setUp(() async {
    server = await FakeRemoteServer.start();
    host = await FakeRemoteHost.start(server);
    host.directResponseLimit = 1024;
    account = RemoteAccount(secrets: MemorySecretStore(), serverUrl: server.url);
  });

  tearDown(() async {
    try {
      await account.close();
      await host.close();
      await server.close();
    } finally {
      expect(server.protocolErrors, isEmpty);
    }
  });

  Future<DshClient> connect() async {
    await account.loginWithPassword(FakeRemoteServer.account, FakeRemoteServer.password);
    final target = (await account.listHosts()).single;
    return account.connectHost(target, muxOptions: fastMux);
  }

  test('RemoteTunnelCarrier exposes DsBotRpcTransport for dsBot/call and dsBot/feed', () async {
    final client = await connect();
    final api = DsBotApi.of(client);
    expect(api, isNotNull);
    expect(api!.transport, isA<DsBotRpcTransport>());

    host.handlers['dsBot/call'] = (args) {
      expect(args['endpoint'], 'hello');
      return {
        'ok': true,
        'value': {
          'ok': true,
          'value': {
            'api': 1,
            'apiMin': 1,
            'apiMax': 1,
            'plugin': {'name': 'ds-bot', 'version': '0.3.10'},
            'capabilities': ['rpc'],
            'boot': 'b1',
            'seq': 0,
            'revision': 2,
            'readOnly': null,
            'memory': true,
            'feed': 'api/bot/feed',
            'rpc': {'call': 'dsBot/call', 'feed': 'dsBot/feed'},
          },
        },
      };
    };

    final hello = await api.hello();
    expect(hello.api, 1);
    expect(hello.supports('rpc'), isTrue);
    expect(hello.revision, 2);

    host.onOpen = (stream) {
      if (stream.endpoint != 'dsBot/feed') return;
      scheduleMicrotask(() {
        stream.push({
          'event': 'hello',
          'data': {'revision': 2, 'api': 1, 'boot': 'b1', 'seq': 0},
        });
        stream.push({
          'event': 'notice',
          'data': {
            'seq': 1,
            'kind': 'message',
            'botId': 'b1',
            'name': 'Chief',
            'text': 'ready',
            'time': 1786000000000,
          },
        });
      });
    };

    final feed = api.feed(backoff: const [Duration(milliseconds: 20)]);
    final events = <DsBotFeedEvent>[];
    final gotNotice = Completer<void>();
    final sub = feed.events.listen((event) {
      events.add(event);
      if (event is DsBotFeedNotice && !gotNotice.isCompleted) gotNotice.complete();
    });
    feed.start();
    await gotNotice.future.timeout(const Duration(seconds: 5));
    await sub.cancel();
    await feed.close();

    expect(events.map((e) => e.runtimeType), [DsBotFeedHello, DsBotFeedNotice]);
    expect((events[1] as DsBotFeedNotice).notice.text, 'ready');
  });

  test('an oversized mutating dsBot/call response is not dispatched twice', () async {
    final client = await connect();
    final api = DsBotApi.of(client)!;
    final bigPayload = 'x' * 4096;
    var calls = 0;
    host.handlers['dsBot/call'] = (args) {
      expect(args['endpoint'], 'delete-bot');
      calls++;
      return {
        'ok': true,
        'value': {
          'ok': true,
          'value': {'revision': calls, 'blob': bigPayload},
        },
      };
    };

    Object? outcome;
    try {
      await api.deleteBot('b1');
    } catch (error) {
      outcome = error;
    }

    expect(calls, 1);
    expect(outcome, isA<DshRemoteException>().having((e) => e.code, 'code', 'RESPONSE_TOO_LARGE'));
    expect(host.rpcMethods.where((m) => m.startsWith('harness.remote.transfer.')), isEmpty);
  });

  test('an oversized read-only dsBot/call response falls back to chunked transfer', () async {
    final client = await connect();
    final api = DsBotApi.of(client)!;
    final bigPreview = 'x' * 4096;
    var calls = 0;
    host.handlers['dsBot/call'] = (args) {
      expect(args['endpoint'], 'file-text');
      calls++;
      return {
        'ok': true,
        'value': {
          'ok': true,
          'value': {'text': bigPreview, 'whole': true},
        },
      };
    };

    final read = await api.readFileText(const DsBotLongText(100, {'attachmentId': 'a1'}));
    expect(read.text.length, bigPreview.length);
    expect(read.whole, isTrue);
    expect(calls, 2);
    expect(host.rpcMethods.where((m) => m == 'harness.remote.transfer.commit'), hasLength(1));
  });
}
