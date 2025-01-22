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
                , WORKFLOW_NAME='PP_CSNG724B',WORKFLOW_ID='c1b8e5ee139a4d8aac0ef45fb73d8d72', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG724B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 6, 28, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('c1b8e5ee139a4d8aac0ef45fb73d8d72')

    csng724bJob_vol = []
    csng724bJob_volMnt = []
    csng724bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csng724bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    csng724bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng724bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '794a2a65449d44639aaaa6369fe7704e',
        'volumes': csng724bJob_vol,
        'volume_mounts': csng724bJob_volMnt,
        'env_from':csng724bJob_env,
        'task_id':'csng724bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.123',
        'arguments':["--job.name=csng724bJob", "inDate=${YYYYMM}", "inSaCd=2C48" ,"ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('c1b8e5ee139a4d8aac0ef45fb73d8d72')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng724bJob,
        Complete
    ]) 

    # authCheck >> csng724bJob >> Complete
    workflow








