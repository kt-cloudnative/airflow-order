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
                , WORKFLOW_NAME='TC_CBOL022B',WORKFLOW_ID='ad6e366a36154da780bb10c308158e42', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-TC_CBOL022B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 26, 17, 55, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('ad6e366a36154da780bb10c308158e42')

    cbol022bJob_vol = []
    cbol022bJob_volMnt = []
    cbol022bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol022bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol022bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol022bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'abdc79b8b4974c31b7ba4d30c6a8ccbc',
        'volumes': cbol022bJob_vol,
        'volume_mounts': cbol022bJob_volMnt,
        'env_from':cbol022bJob_env,
        'task_id':'cbol022bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol022bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('ad6e366a36154da780bb10c308158e42')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol022bJob,
        Complete
    ]) 

    # authCheck >> cbol022bJob >> Complete
    workflow








