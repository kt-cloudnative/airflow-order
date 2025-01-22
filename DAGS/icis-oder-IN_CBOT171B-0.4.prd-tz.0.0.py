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
                , WORKFLOW_NAME='IN_CBOT171B',WORKFLOW_ID='35126d2d818746b7b72e80822453c1ce', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOT171B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 36, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('35126d2d818746b7b72e80822453c1ce')

    cbot171bJob_vol = []
    cbot171bJob_volMnt = []
    cbot171bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbot171bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbot171bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbot171bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbot171bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbot171bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'd4e1fd8664844c86be8b0ffad1857721',
        'volumes': cbot171bJob_vol,
        'volume_mounts': cbot171bJob_volMnt,
        'env_from':cbot171bJob_env,
        'task_id':'cbot171bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.6',
        'arguments':["--job.name=cbot171bJob", "inDate=${YYYYMM}01", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('35126d2d818746b7b72e80822453c1ce')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbot171bJob,
        Complete
    ]) 

    # authCheck >> cbot171bJob >> Complete
    workflow








