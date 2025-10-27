use crate::workflow::wasi::wasi::sockets;
use crate::workflow::workflow_ctx::WorkflowCtx;
use concepts::time::ClockFn;

impl<C: ClockFn> sockets::network::Host for WorkflowCtx<C> {
    fn network_error_code(
        &mut self,
        _err: wasmtime::component::Resource<sockets::network::Error>,
    ) -> wasmtime::Result<Option<sockets::network::ErrorCode>> {
        Ok(None)
    }
}

impl<C: ClockFn> sockets::network::HostNetwork for WorkflowCtx<C> {
    fn drop(
        &mut self,
        _rep: wasmtime::component::Resource<sockets::network::Network>,
    ) -> wasmtime::Result<()> {
        Ok(())
    }
}

impl<C: ClockFn> sockets::instance_network::Host for WorkflowCtx<C> {
    fn instance_network(
        &mut self,
    ) -> wasmtime::Result<wasmtime::component::Resource<sockets::instance_network::Network>> {
        Err(wasmtime::Error::msg(
            "wasi:network/instance-network is stubbed",
        ))
    }
}

mod udp {
    use super::{ClockFn, WorkflowCtx, sockets};
    use sockets::udp::{
        ErrorCode, IncomingDatagramStream, IpAddressFamily, IpSocketAddress, Network,
        OutgoingDatagramStream, Pollable, UdpSocket,
    };

    impl<C: ClockFn> sockets::udp::Host for WorkflowCtx<C> {}

    impl<C: ClockFn> sockets::udp::HostOutgoingDatagramStream for WorkflowCtx<C> {
        fn check_send(
            &mut self,
            _self_: wasmtime::component::Resource<sockets::udp::OutgoingDatagramStream>,
        ) -> wasmtime::Result<Result<u64, sockets::udp::ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn send(
            &mut self,
            _self_: wasmtime::component::Resource<sockets::udp::OutgoingDatagramStream>,
            _datagrams: wasmtime::component::__internal::Vec<sockets::udp::OutgoingDatagram>,
        ) -> wasmtime::Result<Result<u64, sockets::udp::ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn subscribe(
            &mut self,
            _self_: wasmtime::component::Resource<sockets::udp::OutgoingDatagramStream>,
        ) -> wasmtime::Result<wasmtime::component::Resource<sockets::udp::Pollable>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn drop(
            &mut self,
            _rep: wasmtime::component::Resource<sockets::udp::OutgoingDatagramStream>,
        ) -> wasmtime::Result<()> {
            Ok(())
        }
    }
    impl<C: ClockFn> sockets::udp::HostIncomingDatagramStream for WorkflowCtx<C> {
        fn receive(
            &mut self,
            _self_: wasmtime::component::Resource<sockets::udp::IncomingDatagramStream>,
            _max_results: u64,
        ) -> wasmtime::Result<
            Result<
                wasmtime::component::__internal::Vec<sockets::udp::IncomingDatagram>,
                sockets::udp::ErrorCode,
            >,
        > {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn subscribe(
            &mut self,
            _self_: wasmtime::component::Resource<sockets::udp::IncomingDatagramStream>,
        ) -> wasmtime::Result<wasmtime::component::Resource<sockets::udp::Pollable>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn drop(
            &mut self,
            _rep: wasmtime::component::Resource<sockets::udp::IncomingDatagramStream>,
        ) -> wasmtime::Result<()> {
            Ok(())
        }
    }

    impl<C: ClockFn> sockets::udp::HostUdpSocket for WorkflowCtx<C> {
        fn start_bind(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
            _network: wasmtime::component::Resource<Network>,
            _local_address: IpSocketAddress,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn finish_bind(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn stream(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
            _remote_address: Option<IpSocketAddress>,
        ) -> wasmtime::Result<
            Result<
                (
                    wasmtime::component::Resource<IncomingDatagramStream>,
                    wasmtime::component::Resource<OutgoingDatagramStream>,
                ),
                ErrorCode,
            >,
        > {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn local_address(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<IpSocketAddress, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn remote_address(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<IpSocketAddress, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn address_family(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<IpAddressFamily> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn unicast_hop_limit(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<u8, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn set_unicast_hop_limit(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
            _value: u8,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn receive_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<u64, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn set_receive_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
            _value: u64,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn send_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<Result<u64, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn set_send_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
            _value: u64,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn subscribe(
            &mut self,
            _self_: wasmtime::component::Resource<UdpSocket>,
        ) -> wasmtime::Result<wasmtime::component::Resource<Pollable>> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }

        fn drop(&mut self, _rep: wasmtime::component::Resource<UdpSocket>) -> wasmtime::Result<()> {
            Err(wasmtime::Error::msg("wasi:network/udp is stubbed"))
        }
    }
}

mod udp_create_socket {
    use super::{ClockFn, WorkflowCtx, sockets};
    use sockets::udp::{ErrorCode, IpAddressFamily, UdpSocket};

    impl<C: ClockFn> sockets::udp_create_socket::Host for WorkflowCtx<C> {
        fn create_udp_socket(
            &mut self,
            _address_family: IpAddressFamily,
        ) -> wasmtime::Result<Result<wasmtime::component::Resource<UdpSocket>, ErrorCode>> {
            Err(wasmtime::Error::msg(
                "wasi:sockets/udp-create-socket is stubbed",
            ))
        }
    }
}

mod tcp {
    use super::{ClockFn, WorkflowCtx, sockets};
    use sockets::tcp::{
        Duration, ErrorCode, InputStream, IpAddressFamily, IpSocketAddress, Network, OutputStream,
        Pollable, ShutdownType, TcpSocket,
    };

    impl<C: ClockFn> sockets::tcp::Host for WorkflowCtx<C> {}

    impl<C: ClockFn> sockets::tcp::HostTcpSocket for WorkflowCtx<C> {
        fn start_bind(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _network: wasmtime::component::Resource<Network>,
            _local_address: IpSocketAddress,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn finish_bind(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn start_connect(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _network: wasmtime::component::Resource<Network>,
            _remote_address: IpSocketAddress,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn finish_connect(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<
            Result<
                (
                    wasmtime::component::Resource<InputStream>,
                    wasmtime::component::Resource<OutputStream>,
                ),
                ErrorCode,
            >,
        > {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn start_listen(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn finish_listen(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn accept(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<
            Result<
                (
                    wasmtime::component::Resource<TcpSocket>,
                    wasmtime::component::Resource<InputStream>,
                    wasmtime::component::Resource<OutputStream>,
                ),
                ErrorCode,
            >,
        > {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn local_address(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<IpSocketAddress, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn remote_address(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<IpSocketAddress, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn is_listening(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<bool> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn address_family(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<IpAddressFamily> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_listen_backlog_size(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: u64,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn keep_alive_enabled(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<bool, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_keep_alive_enabled(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: bool,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn keep_alive_idle_time(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<Duration, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_keep_alive_idle_time(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: Duration,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn keep_alive_interval(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<Duration, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_keep_alive_interval(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: Duration,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn keep_alive_count(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<u32, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_keep_alive_count(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: u32,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn hop_limit(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<u8, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_hop_limit(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: u8,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn receive_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<u64, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_receive_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: u64,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn send_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<Result<u64, ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn set_send_buffer_size(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _value: u64,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn subscribe(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
        ) -> wasmtime::Result<wasmtime::component::Resource<Pollable>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn shutdown(
            &mut self,
            _self_: wasmtime::component::Resource<TcpSocket>,
            _shutdown_type: ShutdownType,
        ) -> wasmtime::Result<Result<(), ErrorCode>> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }

        fn drop(&mut self, _rep: wasmtime::component::Resource<TcpSocket>) -> wasmtime::Result<()> {
            Err(wasmtime::Error::msg("wasi:sockets/tcp is stubbed"))
        }
    }
}

mod tcp_create_socket {
    use super::{ClockFn, WorkflowCtx, sockets};
    use sockets::tcp_create_socket::{ErrorCode, IpAddressFamily, TcpSocket};

    impl<C: ClockFn> sockets::tcp_create_socket::Host for WorkflowCtx<C> {
        fn create_tcp_socket(
            &mut self,
            _address_family: IpAddressFamily,
        ) -> wasmtime::Result<Result<wasmtime::component::Resource<TcpSocket>, ErrorCode>> {
            Err(wasmtime::Error::msg(
                "wasi:network/tcp-create-socket is stubbed",
            ))
        }
    }
}

mod ip_name_lookup {
    use super::{ClockFn, WorkflowCtx, sockets};
    use sockets::ip_name_lookup::{ErrorCode, IpAddress, Network, Pollable, ResolveAddressStream};

    impl<C: ClockFn> sockets::ip_name_lookup::Host for WorkflowCtx<C> {
        fn resolve_addresses(
            &mut self,
            _network: wasmtime::component::Resource<Network>,
            _name: wasmtime::component::__internal::String,
        ) -> wasmtime::Result<Result<wasmtime::component::Resource<ResolveAddressStream>, ErrorCode>>
        {
            Err(wasmtime::Error::msg(
                "wasi:sockets/ip-name-lookup is stubbed",
            ))
        }
    }

    impl<C: ClockFn> sockets::ip_name_lookup::HostResolveAddressStream for WorkflowCtx<C> {
        fn resolve_next_address(
            &mut self,
            _self_: wasmtime::component::Resource<ResolveAddressStream>,
        ) -> wasmtime::Result<Result<Option<IpAddress>, ErrorCode>> {
            Err(wasmtime::Error::msg(
                "wasi:sockets/ip-name-lookup is stubbed",
            ))
        }

        fn subscribe(
            &mut self,
            _self_: wasmtime::component::Resource<ResolveAddressStream>,
        ) -> wasmtime::Result<wasmtime::component::Resource<Pollable>> {
            Err(wasmtime::Error::msg(
                "wasi:sockets/ip-name-lookup is stubbed",
            ))
        }

        fn drop(
            &mut self,
            _rep: wasmtime::component::Resource<ResolveAddressStream>,
        ) -> wasmtime::Result<()> {
            Err(wasmtime::Error::msg(
                "wasi:sockets/ip-name-lookup is stubbed",
            ))
        }
    }
}
