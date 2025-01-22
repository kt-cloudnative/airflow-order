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
                , WORKFLOW_NAME='PP_CBOA123B',WORKFLOW_ID='2758ccfe008842d395b060adfadc6239', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBOA123B-0.0.prd-tz.7.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 12, 23, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('2758ccfe008842d395b060adfadc6239')

    cboa123bJob_vol = []
    cboa123bJob_volMnt = []
    cboa123bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cboa123bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cboa123bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cboa123bJob_env.extend([getICISConfigMap('icis-oder-ppon-batch-mng-configmap'), getICISSecret('icis-oder-ppon-batch-mng-secret'), getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISSecret('icis-oder-ppon-batch-secret')])
    cboa123bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cboa123bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '255e128771e04095b8ca446693917cbe',
        'volumes': cboa123bJob_vol,
        'volume_mounts': cboa123bJob_volMnt,
        'env_from':cboa123bJob_env,
        'task_id':'cboa123bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.132',
        'arguments':["--job.name=cboa123bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('2758ccfe008842d395b060adfadc6239')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cboa123bJob,
        Complete
    ]) 

    # authCheck >> cboa123bJob >> Complete
    workflow








