from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CBOG828B',WORKFLOW_ID='b9658c0989774b468bb252362f9888f2', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOG828B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 10, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('b9658c0989774b468bb252362f9888f2')

    cbog828bJob_vol = []
    cbog828bJob_volMnt = []
    cbog828bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog828bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog828bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog828bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'eb3c05e9bd6a4560a1e1012dc25755bc',
        'volumes': cbog828bJob_vol,
        'volume_mounts': cbog828bJob_volMnt,
        'env_from':cbog828bJob_env,
        'task_id':'cbog828bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog828bJob", "month=${YYYYMM}", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_1_vol = []
    cbog829bJob_1_volMnt = []
    cbog829bJob_1_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_1_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_1_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_1 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e071186cb7d14d12b58be84554bba101',
        'volumes': cbog829bJob_1_vol,
        'volume_mounts': cbog829bJob_1_volMnt,
        'env_from':cbog829bJob_1_env,
        'task_id':'cbog829bJob_1',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=1","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_2_vol = []
    cbog829bJob_2_volMnt = []
    cbog829bJob_2_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_2_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_2_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_2 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '79f077b7073943649a4d2f197c2614aa',
        'volumes': cbog829bJob_2_vol,
        'volume_mounts': cbog829bJob_2_volMnt,
        'env_from':cbog829bJob_2_env,
        'task_id':'cbog829bJob_2',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=2","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_3_vol = []
    cbog829bJob_3_volMnt = []
    cbog829bJob_3_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_3_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_3_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_3 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '996e747293364e9bb6ed5d17dfdbeda1',
        'volumes': cbog829bJob_3_vol,
        'volume_mounts': cbog829bJob_3_volMnt,
        'env_from':cbog829bJob_3_env,
        'task_id':'cbog829bJob_3',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=3","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_4_vol = []
    cbog829bJob_4_volMnt = []
    cbog829bJob_4_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_4_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_4_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_4 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '79db471507ea4920919854db93b9572d',
        'volumes': cbog829bJob_4_vol,
        'volume_mounts': cbog829bJob_4_volMnt,
        'env_from':cbog829bJob_4_env,
        'task_id':'cbog829bJob_4',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=4","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_5_vol = []
    cbog829bJob_5_volMnt = []
    cbog829bJob_5_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_5_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_5_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_5 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'f2d7ae777c19495ba0d5129d90a3df08',
        'volumes': cbog829bJob_5_vol,
        'volume_mounts': cbog829bJob_5_volMnt,
        'env_from':cbog829bJob_5_env,
        'task_id':'cbog829bJob_5',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=5","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    cbog829bJob_6_vol = []
    cbog829bJob_6_volMnt = []
    cbog829bJob_6_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbog829bJob_6_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cbog829bJob_6_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbog829bJob_6 = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'b1dae47762094187891ec470646ce265',
        'volumes': cbog829bJob_6_vol,
        'volume_mounts': cbog829bJob_6_volMnt,
        'env_from':cbog829bJob_6_env,
        'task_id':'cbog829bJob_6',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cbog829bJob" , "procNo=5","requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('b9658c0989774b468bb252362f9888f2')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbog828bJob,
        cbog829bJob_1,
        cbog829bJob_2,
        cbog829bJob_3,
        cbog829bJob_4,
        cbog829bJob_5,
        cbog829bJob_6,
        Complete
    ]) 

    # authCheck >> cbog828bJob >> cbog829bJob_1 >> cbog829bJob_2 >> cbog829bJob_3 >> cbog829bJob_4 >> cbog829bJob_5 >> cbog829bJob_6 >> Complete
    workflow








