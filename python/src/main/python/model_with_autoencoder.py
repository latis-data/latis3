import datetime
import os
import pandas as pd
from pandas import datetime
from pandas import read_csv
import numpy as np
import matplotlib
matplotlib.use('agg')
from matplotlib import pyplot
from sklearn import preprocessing
from sklearn.preprocessing import normalize
import numpy as np
# import keras
# from keras.callbacks import TensorBoard
# from keras.layers.advanced_activations import LeakyReLU
# from keras.layers import Input, Dense
# from keras.models import Model

# import tensorflow
# #from tensorflow import set_random_seed
from tensorflow.keras.models import Model, load_model,Sequential
from tensorflow.keras.layers import Input, Dense
from tensorflow.keras.callbacks import ModelCheckpoint, TensorBoard
from tensorflow.keras import regularizers


__author__ = 'Shawn Polson'
__contact__ = 'shawn.polson@colorado.edu'


def time_parser(x):
    # Assume the time is given in ms since 1970-01-01
    return pd.to_datetime(x, unit='ms', origin='unix')

def chunk(ts, window_size=18):
    remainder = len(ts) % window_size  # if ts isn't divisible by window_size, drop the first [remainder] data points

    chunked_ts = np.array([ts.values[remainder:window_size+remainder]])
    for i in range(window_size+remainder, len(ts), window_size):
        chunk = []
        for j in range(i, i+window_size):
            chunk.append(ts.values[j])
        chunked_ts = np.append(chunked_ts, [chunk], axis=0)

    return chunked_ts


# Given a chunked time series, return a list of floats that are the predictions
def get_autoencoder_predictions(encoder, decoder, ts):
    predictions = []

    for i in range(len(ts)):
        inputs = np.array([ts[i]])
        x = encoder.predict(inputs)  # the compressed representation
        y = decoder.predict(x)[0]    # the decoded output
        predictions = predictions + y.tolist()

    return predictions


# Given a chunked time series, return a numpy array where each row is an array of 10 floats (the feature vectors).
def get_compressed_feature_vectors(encoder, ts):
    compressed_feature_vectors = []

    for i in range(len(ts)):
        inputs = np.array([ts[i]])
        x = encoder.predict(inputs)  # the compressed representation
        compressed_feature_vectors = compressed_feature_vectors + x.tolist()

    return np.array(compressed_feature_vectors)


class AutoEncoder:
    def __init__(self, train_data, encoding_dim=3, verbose=False):
        self.encoding_dim = encoding_dim
        r = lambda: np.random.randint(1, 3)
        # self.x = np.array([[r(), r(), r()] for _ in range(1000)])
        # self.x = np.array([[r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r(), r()] for _ in range(1000)])
        self.x = train_data
        # print(self.x)
        self.verbose = 0
        if verbose:
            self.verbose = 1

    def _encoder(self):
        inputs = Input(shape=self.x[0].shape)
        #h1 = Dense(150, activation='tanh')(inputs)
        h2 = Dense(75, activation='tanh')(inputs)
        encoded = Dense(self.encoding_dim, activation='tanh')(h2)
        model = Model(inputs, encoded)
        self.encoder = model
        return model

    def _decoder(self):
        inputs = Input(shape=(self.encoding_dim,))
        h3 = Dense(75, activation='tanh')(inputs)
        # h4 = Dense(150, activation='tanh')(h3)
        num_outputs = self.x[0].shape[0]
        decoded = Dense(num_outputs)(h3)
        model = Model(inputs, decoded)
        self.decoder = model
        return model

    def encoder_decoder(self):
        ec = self._encoder()
        dc = self._decoder()

        inputs = Input(shape=self.x[0].shape)
        ec_out = ec(inputs)
        dc_out = dc(ec_out)
        model = Model(inputs, dc_out)

        self.model = model
        # print(self.model.summary())
        return model

    def fit(self, batch_size=10, epochs=300):
        self.model.compile(optimizer='sgd', loss='mse')
        log_dir = './log/'
        tbCallBack = TensorBoard(log_dir=log_dir, histogram_freq=0, write_graph=True, write_images=True)
        self.model.fit(self.x, self.x,
                       epochs=epochs,
                       batch_size=batch_size,
                       callbacks=[tbCallBack],
                       verbose=self.verbose)

    def save(self):
        if not os.path.exists(r'./weights'):
            os.mkdir(r'./weights')
        else:
            self.encoder.save(r'./weights/encoder_weights.h5')
            self.decoder.save(r'./weights/decoder_weights.h5')
            self.model.save(r'./weights/ae_weights.h5')


def autoencoder_prediction(ts, train_size=1.0, col_name='Autoencoder', var_name='Value', ds_name='Dataset',
                           verbose=False, plot_save_path=None, cfv_save_path=None, data_save_path=None,
                           load_model=False, save_model=False):
    """Predict the given time series with an autoencoder.

       Inputs:
           ts [Array[Array[float, float]]: The time series data as an array of arrays.
                                         It becomes a pandas Series with a DatetimeIndex and a column for numerical values.

       Optional Inputs:
           train_size [float]:    The percentage of data to use for training, as a float (e.g., 0.66).
                                  Default is 1.0.
           col_name [str]:        The name of the autoencoder column.
                                  Default is 'Autoencoder'.
           var_name [str]:        The name of the dependent variable in the time series.
                                  Default is 'Value'.
           ds_name [str]:         The name of the dataset.
                                  Default is 'Dataset'.
           cfv_save_path [str]:   The path to the root directory where the compressed feature vectors can be saved.
           plot_save_path [str]:  The path to the root directory where a plot of the autoencoder prediction can be saved.
           data_save_path [str]:  The path to the root directory where the autoencoder predictions can be saved as a CSV.
           load_model [bool]:     When True, try to load a model with pre-trained weights and use it.
           save_model [bool]:     When True, the autoencoder weights will be saved to disk in a 'weights' directory.
           verbose [bool]:        When True, describe the time series dataset upon loading it, and pass 'verbose=True' down the chain to any other functions called during outlier detection.
                                  Default is False.

       Outputs:
            ts_with_autoencoder [pd DataFrame]: The original time series with an added column for this autoencoer's predictions.

       Optional Outputs:
           None

       Example:
           time_series_with_autoencoder = autoencoder_prediction(dataset_path=dataset, ds_name, train_size=0.5, var_name=name,
                                                                 verbose=True)
       """

    # Load the dataset
    time_series = pd.DataFrame(data=ts, columns=['Time', var_name])
    time_series['Time'] = time_series['Time'].apply(lambda time: time_parser(time))  # convert times to datetimes
    time_series = time_series.set_index('Time')                                      # set the datetime column as the index
    time_series = time_series.squeeze()                                              # convert to a Series

    # Normalize data values between 0 and 1
    X = time_series.values.reshape(-1, 1)
    min_max_scaler = preprocessing.MinMaxScaler()
    X_scaled = min_max_scaler.fit_transform(X).reshape(1, -1).tolist()[0]
    normalized = pd.Series(X_scaled, index=time_series.index)
    time_series = normalized

    # Chunk the dataset
    window_size = 18
    chunked_ts = chunk(time_series, window_size)

    # Split into train and test sets
    split = int(len(chunked_ts) * train_size)
    train, test = chunked_ts[:split], chunked_ts[split:]

    # Store un-chunked test set for plotting
    split = int(len(time_series) * train_size)
    unchunked_test = time_series[split:]

    if load_model:
        encoder = load_model(r'weights/encoder_weights.h5')
        decoder = load_model(r'weights/decoder_weights.h5')
    else:
        ae = AutoEncoder(train, encoding_dim=10, verbose=verbose)  # Note, training autoencoder just with train data
        ae.encoder_decoder()
        ae.fit(batch_size=50, epochs=1000)
        if save_model:
            ae.save()

        encoder = ae.encoder
        decoder = ae.decoder

    predictions = time_series.values[:len(time_series)%window_size].tolist()
    autoencoder_predictions = get_autoencoder_predictions(encoder, decoder, chunked_ts)  # note, network won't have seen test portion
    predictions = predictions + autoencoder_predictions
    predictions = pd.Series(predictions, index=time_series.index)

    if plot_save_path is not None:
        # Save plot
        ax = time_series.plot(color='#192C87', title=ds_name + ' with Autoencoder Predictions', label=var_name, figsize=(14, 6))
        if len(unchunked_test) > 0:
            unchunked_test.plot(color='#441594', label='Test Data')
        predictions.plot(color='#0CCADC', label='Autoencoder Predictions', linewidth=1)
        ax.set(xlabel='Time', ylabel=var_name)
        pyplot.legend(loc='best')

        # save plot before showing it
        plot_filename = ds_name + '_with_autoencoder_' + str(train_size) + '.png'
        if plot_save_path.endswith('/'):
            plot_path = plot_save_path + ds_name + '/autoencoder/plots/' + str(int(train_size * 100)) + ' percent/'
        else:
            plot_path = plot_save_path + '/' + ds_name + '/autoencoder/plots/' + str(int(train_size * 100)) + ' percent/'
        if not os.path.exists(plot_path):
            os.makedirs(plot_path)
        pyplot.savefig(plot_path + plot_filename, dpi=500)

    if cfv_save_path is not None:
        # Save compressed feature vectors
        cfv = get_compressed_feature_vectors(encoder, chunked_ts)
        cfv_filename = ds_name + '_compressed_by_autoencoder_' + str(train_size) + '.npy'
        if cfv_save_path.endswith('/'):
            cfv_path = cfv_save_path + ds_name + '/autoencoder/data/' + str(int(train_size * 100)) + ' percent/'
        else:
            cfv_path = cfv_save_path + '/' + ds_name + '/autoencoder/data/' + str(int(train_size * 100)) + ' percent/'
        np.save(cfv_path + cfv_filename, cfv)

    ts_with_autoencoder = pd.DataFrame({col_name: predictions, var_name: time_series})
    ts_with_autoencoder.rename_axis('Time', axis='index', inplace=True)  # name index 'Time'
    column_names = [var_name, col_name]  # column order
    ts_with_autoencoder = ts_with_autoencoder.reindex(columns=column_names)  # sort columns in specified order

    if data_save_path is not None:
        # Save data to proper directory with encoded file name
        data_filename = ds_name + '_with_autoencoder_' + str(train_size) + '.csv'
        if data_save_path.endswith('/'):
            data_path = data_save_path + ds_name + '/autoencoder/data/' + str(int(train_size * 100)) + ' percent/'
        else:
            data_path = data_save_path + '/' + ds_name + '/autoencoder/data/' + str(int(train_size * 100)) + ' percent/'
        if not os.path.exists(data_path):
            os.makedirs(data_path)
        ts_with_autoencoder.to_csv(data_path + data_filename)

    return ts_with_autoencoder
