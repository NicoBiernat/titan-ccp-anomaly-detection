package titan.ccp.anomalydetection.api;

import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.Response;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public final class FutureCallback implements Callback {

    private CompletableFuture future;

    public FutureCallback(CompletableFuture future) {
        this.future = future;
    }

    @Override
    public void onFailure(Call call, IOException e) {
        e.printStackTrace();
        future.cancel(false);
    }

    @Override
    public void onResponse(Call call, Response response) throws IOException {
        future.complete(response.body().string());
    }
}