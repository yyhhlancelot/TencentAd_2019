import os
import time
import pandas as pd
import gc
filename_static = 'D:/project/Tencent/testA/static/ad_static_feature.out'
filename_op = 'D:/project/Tencent/testA/ad_operation.dat'
filename_log = 'J:/Code/Tencent_ad/2019/testA/log/totalExposureLog.out'
filename_user = 'J:/Code/Tencent_ad/2019/testA/user/user_data'
filename_test = 'D:/project/Tencent/testA/test_sample.dat'
def read_data(filename, list_index, store_route, num_each_csv):
	data_list = []
	with open(filename, 'r') as f:
		count = 0
		for i, line in enumerate(f):
			if i % num_each_csv == 0:
				print(i)
			line = line.strip().split('\t')
			obj_dict = {}
			for j, each in enumerate(line):
				try:
					obj_dict[list_index[j]] = int(each)
				except:
					obj_dict[list_index[j]] = each
			data_list.append(obj_dict)

			if i != 0 and i % num_each_csv == 0:
				data_feature = pd.DataFrame(data_list)
				data_feature.to_csv(store_route + str(count) + '.csv', index = False)
				count += 1
				del data_list, data_feature
				gc.collect()
				data_list = []
		data_feature = pd.DataFrame(data_list)
		data_feature.to_csv(store_route + str(count) + '.csv', index = False)
		count += 1
		del data_list, data_feature
		gc.collect()
		
	

# ad_expose_index = ['request_id', 'request_time', 'loc_id', 'uid', 'aid', 'ad_size', 'ad_bid', 'ad_pctr',
                 # 'ad_quality_ecpm', 'ad_total_ecpm']
# store_route_log = 'J:/Code/Tencent_ad/2019/testA/log/ad_expose_log_'
# read_data(filename_log, ad_expose_index, store_route_log, 1000000)

user_feature_index = ['aid', 'age', 'gender', 'area', 'status', 'education', 'consuption_ability', 
					'device', 'work', 'connection_type', 'behavior']
store_route_user = 'J:/Code/Tencent_ad/2019/testA/user/user_'
read_data(filename_user, user_feature_index, store_route_user, 100000)

# ad_static_feature_index = ['aid', 'build_time', 'ad_account_id', 'goods_id', 'goods_class',
							#'ad_class_id', 'material_size']
#store_route_static = 'D:/project/Tencent/testA/ad_static_feature_'
# read_data(filename_static, ad_static_feature_index, store_route_static, 1000000)

# ad_operation_index = ['aid', 'build/modify_time', 'op_type', 'modify_class', 'modify_val']
# store_route_op = 'D:/project/Tencent/testA/ad_operation_'
# read_data(filename_op, ad_operation_index, store_route_op, 1000000)

# test_feature_index = ['id', 'aid', 'build_time', 'material_size', 'ad_class_id', 'goods_class',
						# 'ad_account_id', 'goods_id', 'advertising_period', 'audience_targeting', 'bid']
# store_route_test = 'D:/project/Tencent/testA/test_feature_'	
# read_data(filename_test, test_feature_index, store_route_test, 1000000)					